import os
import sys
import logging
import numpy as np
import faiss
from dotenv import load_dotenv
import hnsw_utils 

load_dotenv(override=True)

# DATABASE = "wikipedia"
# TABLE_NAME = "dbo.wikipedia_articles_title_embeddings_native"
# ID_COLUMN = "id"
# VECTOR_COLUMN = "title_vector_native"
# VECTOR_SIZE = 1536
# TOP_N = 25000

DATABASE = "vectordb"
TABLE_NAME = "benchmark.vector_768"
ID_COLUMN = "id"
VECTOR_COLUMN = "vector"
VECTOR_SIZE = 768
TOP_N = 1000000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

logging.info("Loading vectors from database...")

ids, vectors = hnsw_utils.load_vectors_from_db(
    table_name=TABLE_NAME,
    id_column=ID_COLUMN,
    vector_column=VECTOR_COLUMN,
    vector_size=VECTOR_SIZE,
    top_n=TOP_N
)

logging.info("Creating HNSW index...")

nvp = np.asarray(vectors)
d = nvp.shape[1]
index = faiss.index_factory(d, "HNSW", faiss.METRIC_INNER_PRODUCT)  
index.add(nvp)
hnsw = index.hnsw

logging.info("Saving HNSW index graph back to database...")

hnsw_utils.save_hnsw_graph_to_db(ids, hnsw)    

logging.info("Done.")