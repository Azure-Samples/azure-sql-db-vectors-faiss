import os
import sys
import pyodbc
import logging
import json
import numpy as np
import faiss
from dotenv import load_dotenv
from sqlext.utils import Buffer, VectorSet, NpEncoder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

load_dotenv()

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

def save_level(l, items):
    logging.info(f"Saving level {l}...")
    query = f"INSERT INTO [$vector].faiss_hnsw_level{l} (id) VALUES (?)"
    
    conn = pyodbc.connect(os.environ["MSSQL"]) 
    cursor = conn.cursor()  
    cursor.fast_executemany = True
    
    cursor.fast_executemany(query, items)

    cursor.commit()
    conn.close()        

def test():
    logging.info("Loading vectors from database...")
    ids, vectors = load_vectors_from_db()

    logging.info("Creating index...")
    nvp = np.asarray(vectors)
    d = nvp.shape[1]
    index = faiss.index_factory(d, "HNSW32")    
    index.add(nvp)

    logging.info("Querying index...")
    qp = np.where(ids == 8698)[0][0]
    logging.info(f"Querying for item id 8698, position {qp}")
    qv = np.asarray([nvp[qp]])    
    logging.info(qv)
    dist, idx = index.search(qv, 10)
    #r = dict(zip([int(i) for i in ids[idx][0]], 1-dist[0]))
    logging.info(ids[idx[0]])
    logging.info(dist[0])

    logging.info("Graph links...")
    hnsw = index.hnsw
    levels = faiss.vector_to_array(hnsw.levels)
    lmin = levels.min()
    lmax = levels.max()
    logging.info(f"Levels: {lmin} -> {lmax}")    
    for l in range(lmax, lmin):
        n = np.where(levels == l)[0]
        logging.info(f"Level {l}: {n} items")   
        save_level(l, n)

    g = get_hnsw_links(hnsw, qp)
    logging.info(ids[[v for v in g[0] if v != -1]])

def vector_to_array(v): 
    """ make a vector visible as a numpy array (without copying data)"""
    return faiss.rev_swig_ptr(v.data(), v.size())

def get_hnsw_links(hnsw, vno): 
    """ get link structure for vertex vno """
    
    # make arrays visible from Python
    levels = vector_to_array(hnsw.levels)
    cum_nneighbor_per_level = vector_to_array(hnsw.cum_nneighbor_per_level)
    offsets = vector_to_array(hnsw.offsets)
    neighbors = vector_to_array(hnsw.neighbors)
    
    # all neighbors of vno
    neigh_vno = neighbors[offsets[vno] : offsets[vno + 1]]
    
    # break down per level 
    nlevel = levels[vno]
    return [
        neigh_vno[cum_nneighbor_per_level[l] : cum_nneighbor_per_level[l + 1]]
        for l in range(nlevel)
    ]                 
    
if __name__ == "__main__":    
    test()
    