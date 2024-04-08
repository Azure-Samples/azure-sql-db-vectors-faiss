import os
import sys
import pyodbc
import logging
import json
import numpy as np
import faiss

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
    
def load_vectors_from_db():    
        conn = pyodbc.connect(os.environ["MSSQL"]) 
        query = "SELECT id AS item_id, title_vector AS vector FROM [dbo].[wikipedia_articles_title_embeddings_native] ORDER BY item_id"
        buffer = Buffer()    
        result = VectorSet(1536)
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
                buffer.add(row.item_id, json.loads(row.vector))
            
            result.add(buffer)
            tr += (idx+1)

            mf = int(result.get_memory_usage() / 1024 / 1024)
            logging.info("Loaded {0} rows, total rows {1}, total memory footprint {2} MB".format(idx+1, tr, mf))        
            
        cursor.close()
        conn.commit()
        conn.close()
        return result.ids, result.vectors

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
    
def save_hnsw_graph(ids, hnsw):
    conn = pyodbc.connect(os.environ["MSSQL"]) 

    cursor = conn.cursor()  
    cursor.fast_executemany = True    
    cursor.execute("TRUNCATE TABLE [$vector].faiss_hnsw")
    cursor.commit()

    #for each vector
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

            cursor.executemany(f'INSERT INTO [$vector].faiss_hnsw (id, id_neighbor, l) VALUES (?, ?, ?)', params)

    cursor.commit()
    conn.close()  