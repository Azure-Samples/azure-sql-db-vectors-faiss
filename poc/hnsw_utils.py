import os
import sys
import pyodbc
import logging
import json
import numpy as np
import faiss
import struct

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

class Buffer:
    def __init__(self):
        self.ids = []
        self.vectors = []
             
    def add(self, id, vector):
        self.ids.append(id)
        self.vectors.append(vector)

    def clear(self):
        self.ids.clear()
        self.vectors.clear()

class VectorSet:
    def __init__(self, vector_dimensions:int):
        self.ids = np.empty((0), dtype=np.int32)
        self.vectors = np.empty((0, vector_dimensions), dtype=np.float32)      
             
    def add(self, buffer:Buffer):
        self.ids = np.append(self.ids, np.asarray(buffer.ids), 0)
        self.vectors = np.append(self.vectors, np.asarray(buffer.vectors, dtype=np.float32), 0)          

    def get_memory_usage(self):
        return self.ids.nbytes + self.vectors.nbytes
    
class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.int32):
            return int(obj)
        if isinstance(obj, np.int64):
            return int(obj)
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        if isinstance(obj, np.float32):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return super(NpEncoder, self).default(obj)

def array_to_vector(a:list[float])->bytearray:
    # header
    b = bytearray([169, 1])

    # number of items
    b += bytearray(struct.pack("i", len(a)))
    pf = f"{len(a)}f"

    # filler
    b += bytearray([0,0])

    # items
    b += bytearray(struct.pack(pf, *a))

    return b

def vector_to_array(b:bytearray)->list[float]:
    # header
    h = struct.unpack_from("2B", b, 0)    
    assert h == (169,1)

    c = int(struct.unpack_from("i", b, 2)[0])
    pf = f"{c}f"
    a = struct.unpack_from(pf, b, 8)
    return a

def load_vectors_from_db(table_name:str, id_column:str, vector_column:str, vector_size:int, top_n:int):    
    conn = pyodbc.connect(os.environ["MSSQL"]) 
    query = "SELECT "
    if (top_n): query += f"TOP {top_n} "
    query += f"[{id_column}] AS item_id, [{vector_column}] AS vector FROM {table_name} ORDER BY item_id"
    buffer = Buffer()    
    result = VectorSet(vector_size)
    cursor = conn.cursor()
    cursor.execute(query)
    tr = 0
    while(True):
        buffer.clear()    
        rows = cursor.fetchmany(10000)
        if (rows == []):
            logging.info("Done")
            break

        for idx, row in enumerate(rows):
            #buffer.add(row.item_id, json.loads(row.vector))
            buffer.add(row.item_id, vector_to_array(row.vector))
        
        result.add(buffer)
        tr += (idx+1)

        mf = int(result.get_memory_usage() / 1024 / 1024)
        logging.info("Loaded {0} rows, total rows {1}, total memory footprint {2} MB".format(idx+1, tr, mf))        
        
    cursor.close()
    conn.commit()
    conn.close()
    return result.ids, result.vectors

# from here: https://github.com/facebookresearch/faiss/wiki/Python-C---code-snippets#how-can-i-get-the-link-structure-of-a-hnsw-index
def get_hnsw_links(hnsw, vno): 
    """ get link structure for vertex vno """
    
    # make arrays visible from Python
    levels = faiss.vector_to_array(hnsw.levels)
    cum_nneighbor_per_level = faiss.vector_to_array(hnsw.cum_nneighbor_per_level)
    offsets = faiss.vector_to_array(hnsw.offsets)
    neighbors = faiss.vector_to_array(hnsw.neighbors)
    
    # all neighbors of vno
    neigh_vno = neighbors[offsets[vno] : offsets[vno + 1]]
    
    # break down per level 
    nlevel = levels[vno]
    return [
        neigh_vno[cum_nneighbor_per_level[l] : cum_nneighbor_per_level[l + 1]]
        for l in range(nlevel)
    ]                 

def save_hnsw_graph_to_db_old(ids, hnsw):
    conn = pyodbc.connect(os.environ["MSSQL"]) 
    cursor = conn.cursor()   

    cursor.execute("truncate table [$vector].[faiss_hnsw]")
    cursor.commit()

    cursor.execute("drop index ixc on [$vector].[faiss_hnsw]")
    cursor.commit()

    cursor.execute(f""" 
        if type_id('dbo.faiss_hnsw_payload') is null begin
            create type dbo.faiss_hnsw_payload as table
            (
                [id] [int] not null, 
                [id_neighbor] [int] not null, 
                [l] [int] not null
            )
        end
    """)
    cursor.commit()

    cursor.execute(f""" 
            create or alter procedure dbo.stp_load_faiss_hnsw
            @dummy int,
            @payload dbo.faiss_hnsw_payload readonly
            as
            begin
                set nocount on
                insert into [$vector].faiss_hnsw (id, id_neighbor, l) select id, id_neighbor, l from @payload;
            end
        """)
    cursor.commit()

    #for each vector
    tvp = []
    for i in range(len(ids)):
        # vector id
        id = ids[i]

        # get neighbors
        neighbors = get_hnsw_links(hnsw, i)

        # store all neighbors along with the level
        for lidx, nl in enumerate(neighbors):
            
            # get ids of neighbors
            nl_ids = ids[[v for v in nl if v != -1]]

            # insert into database
            params = [(int(id), int(nv_id), lidx) for nv_id in nl_ids]
            tvp.extend(params)

            if len(tvp) >= 10000:
                logging.info(f"Inserting {len(tvp)} records...")
                cursor.execute("EXEC dbo.stp_load_faiss_hnsw @dummy=?, @payload=?", (1, tvp))  
                cursor.commit()
                tvp = []   
                
    if len(tvp) > 0:
        logging.info(f"Inserting {len(tvp)} records...")
        cursor.execute("EXEC dbo.stp_load_faiss_hnsw @dummy=?, @payload=?", (1, tvp))  
        cursor.commit()
        tvp = []
    
    logging.info(f"Creating index...")
    cursor.execute("create clustered index ixc on [$vector].[faiss_hnsw] (l, id)  with (data_compression = page);")
    cursor.commit()
    
    conn.close()  

    logging.info(f"Done.")
    
def save_hnsw_graph_to_db(ids, hnsw):
    conn = pyodbc.connect(os.environ["MSSQL"]) 
    cursor = conn.cursor()   

    logging.info("Preparing database...")

    cursor.execute("truncate table [$vector].[faiss_hnsw]")
    cursor.commit()

    cursor.execute("drop index ixc on [$vector].[faiss_hnsw]")
    cursor.commit()

    cursor.execute(f""" 
        if type_id('dbo.faiss_hnsw_payload') is null begin
            create type dbo.faiss_hnsw_payload as table
            (
                [id] [int] not null, 
                [id_neighbor] [int] not null, 
                [l] [int] not null
            )
        end
    """)
    cursor.commit()

    cursor.execute(f""" 
            create or alter procedure dbo.stp_load_faiss_hnsw
            @dummy int,
            @payload dbo.faiss_hnsw_payload readonly
            as
            begin
                set nocount on
                insert into [$vector].faiss_hnsw (id, id_neighbor, l) select id, id_neighbor, l from @payload;
            end
        """)
    cursor.commit()

    logging.info("Analyzing HNSW index...")

    # Information of the max level at which each vector is found. One entry per vector 
    levels = faiss.vector_to_array(hnsw.levels)

    # A flat array with all the neighbors of all the vectors
    neighbors = faiss.vector_to_array(hnsw.neighbors)

    # The offset in the neighbors array where the neighbors of each vector start
    offsets = faiss.vector_to_array(hnsw.offsets)

    # Within the offset, the position at which each level starts
    neighbors_per_level = faiss.vector_to_array(hnsw.cum_nneighbor_per_level)
    
    t = len([n for n in neighbors if n >= 0])
    logging.info(f"Inserting {t} records...")

    # for each vector
    p = []
    r = 0;
    for i in range(len(ids)):
        # vector id
        id = ids[i]

        # get all neighbors
        vn = neighbors[offsets[i] : offsets[i + 1]]

        # for each level
        for l in range(levels[i]):
            # get neighbors at level
            vnl = vn[neighbors_per_level[l] : neighbors_per_level[l + 1]]

            p += [(int(id), int(ids[v]), l) for v in vnl if v != -1]

            if len(p) >= 10000:
                r += len(p)
                logging.info(f"Inserting {len(p)} records ({r} of {t})...")
                cursor.execute("EXEC dbo.stp_load_faiss_hnsw @dummy=?, @payload=?", (1, p))  
                cursor.commit()
                p = []   
                
    if len(p) > 0:
        r += len(p)
        logging.info(f"Inserting {len(p)} records ({r} of {t})...")
        cursor.execute("EXEC dbo.stp_load_faiss_hnsw @dummy=?, @payload=?", (1, p))  
        cursor.commit()
        p = []
    
    logging.info(f"{r} record inserted.")

    logging.info(f"Creating index...")
    cursor.execute("create clustered index ixc on [$vector].[faiss_hnsw] (l, id)  with (data_compression = page);")
    cursor.commit()
    
    conn.close()  
