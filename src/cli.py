print("cli.py")

import os
import argparse
import numpy as np
import requests
import json
import faiss
import mssql.mssql_faiss 
import mssql.mssql_kmeans
from mssql import check_db_connection
from dotenv import load_dotenv
from mssql.utils import NpEncoder

load_dotenv()
    
def get_embeddings(text):
    OPENAPI_URL = os.environ["OPENAPI_URL"]
    OPENAPI_KEY = os.environ["OPENAPI_KEY"]

    url = OPENAPI_URL
    data = {"input": text}
    headers = {"api-key": OPENAPI_KEY}
    response = requests.post(url, json=data, headers=headers)
    assert response.ok
    result = response.json()
    e = result["data"][0]["embedding"]       
    return e

def test_faiss(text = None, index_num:int = None, use_saved_index:bool = True):
    index:faiss.IndexFlat = None
    index_num = index_num or os.environ["DEFAULT_INDEX_MODEL_ID"] or 1

    print(f"Using index #{index_num}...")

    if (use_saved_index):
        index = mssql.mssql_faiss.load_index_from_db(index_num)
        if (index == None):
            print(f"Index #{index_num} not found, creating...")
    
    if index == None:       
        print("Creating index...") 
        index = mssql.mssql_faiss.create_index_from_data()    
        mssql.mssql_faiss.save_index_to_db(index, index_num)

    if (text):
        print(f"Getting most similar vector for '{text}'...")
        e = get_embeddings(text)
        qv = np.asarray([e])
        dist, ids = index.search(qv, 10)
        r = dict(zip([str(i) for i in ids[0]], 1-dist[0]))
        print(json.dumps(r, cls=NpEncoder.NpEncoder))


def test_kmeans(text = None, model_num:int = None, use_saved_model:bool = True):
    km:mssql.mssql_kmeans.KMeansModel = None
    model_num = model_num or os.environ["DEFAULT_INDEX_MODEL_ID"] or 1

    print(f"Using model #{model_num}...")

    if (use_saved_model):
        km = mssql.mssql_kmeans.load_model_from_db(model_num)
        if (km == None):
            print(f"Model #{model_num} not found, creating...")
    
    if km == None:        
        print("Creating model...")
        km = mssql.mssql_kmeans.create_model_from_data()    
        mssql.mssql_kmeans.save_model_to_db(km, model_num, 0)
        mssql.mssql_kmeans.save_centroids_to_db(km)
        mssql.extract_vectors_values()
        mssql.create_similarity_function()

    print(f"Clusters: {len(km.model.cluster_centers_)}")

    if (text):
        print(f"Getting cluster for '{text}'...")
        e = get_embeddings(text)
        qv = np.asarray([e])
        l = km.model.predict(qv)    
        print(json.dumps(l, cls=NpEncoder.NpEncoder))

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--env-file', help=".env file to use", required=False)
    parser.add_argument('--text', help="text to use for similarity search", required=False)
    parser.add_argument('--new', help="create a new index/model", action="store_true", default=False) 
    parser.add_argument('--type', help="what type of index/model to use", choices=['faiss', 'kmeans'], required=True) 
    parser.add_argument('--num', help="index db #")
    args = parser.parse_args() 
    print(args)
    
    if args.env_file:
        print(f"loading env file '{args.env_file}'...")
        load_dotenv(args.env_file, override=True)

    check_db_connection()
    
    if args.type == "faiss":
        test_faiss(args.text, args.num, not args.new)
    
    if args.type == "kmeans":
        test_kmeans(args.text, args.num, not args.new)

