import json
import os
import pickle
import logging
import numpy as np
from .index import BaseIndex
from .database import DatabaseEngine, DatabaseEngineException
from .utils import NpEncoder, IndexStatus, IndexSubStatus, UpdateResult, DataSourceConfig
import faiss

_logger = logging.getLogger("uvicorn")

class FaissIndex(BaseIndex):
    def __init__(self) -> None:
        super().__init__()
        self.index = None
        self._db = None
        self._data_version:int = 0
        self._saved_data_version:int = 0

    def from_config(config:DataSourceConfig):
        index = FaissIndex()
        index._db = DatabaseEngine.from_config(config)
        return index

    def from_id(id:int):
        index = FaissIndex()
        index._db = DatabaseEngine.from_id(id)
        return index
    
    def initialize_build(self, force: bool)->int:
        id = None
        try:
            self._db.initialize();
            id = self._db.create_index_metadata(force)
            self.id = id
            _logger.info(f"Index has id {id}.")
        except DatabaseEngineException as e:
            raise Exception(f"Error initializing index: {str(e)}")
        return id
    
    def build(self):
        if (self.id == None):
            raise Exception("Index has not been initialized.")

        try:
            self.index = None
            
            _logger.info(f"Starting create index...")

            _logger.info("Loading data...")
            self._db.update_index_metadata("LOADING_DATA")
            version, ids, vectors = self._db.load_vectors_from_db()
            
            _logger.info("Creating index...")
            self._db.update_index_metadata("FAISS_INDEX_BUILD")
            nvp = np.asarray(vectors)
            vector_count:int = np.shape(nvp)[0]
            dimensions_count:int = np.shape(nvp)[1]
        
            d = nvp.shape[1]
            index = faiss.IndexFlatIP(d)
            
            #nlist = 100
            #ivf = faiss.IndexIVFFlat(index, d, nlist)
            #ivf.train(nvp)
            #ivf.nprobe = 4
            
            index_map = faiss.IndexIDMap(index)
            index_map.add_with_ids(nvp, ids)
            _logger.info(f"Index #{self.id} ({type(index_map)}) created.")
            self.index = index_map
            self._data_version = version

            _logger.info(f"Finalizing index #{self.id} metadata...")
            self._db.finalize_index_metadata(vectors_count)
            _logger.info(f"Done finalizing metadata.")

            _logger.info(f"Index #{self.id} created.")
        except Exception as e:  
            self._db.update_index_metadata("ERROR_DURING_CREATION")
            raise e    

    def load(self):
        self.status = IndexStatus.LOADING
        self.substatus: IndexSubStatus.NONE
        self.index = None
        
        _logger.info(f"Loading index #{self._index_num}...")
        
        pkl, version = self._db.load_index(self._index_num)   
        
        if pkl == None:
            _logger.info("No index found.")
        else:
            self.index = pickle.loads(pkl)
            _logger.info(f"Done loading index #{self._index_num}.")

        if (self.index):
            self._data_version = version
            self._saved_data_version = version
            self.status = IndexStatus.TRAINED
            self.substatus = IndexSubStatus.READY
        else:
            self._data_version = 0
            self._saved_data_version = 0
            self.status = IndexStatus.NOINDEX
            self.substatus = IndexSubStatus.NONE

    def save(self):
        if not (self.status == IndexStatus.TRAINED and 
            self.substatus == IndexSubStatus.READY and
            self._data_version != self._saved_data_version):
            _logger.info("Index already saved and no changes detected, skipping save request.")
            return
        
        _logger.info(f"Saving index #{self._index_num}...")
        self.substatus = IndexSubStatus.SAVING
        pkl = pickle.dumps(self.index)
        self._db.save_index(
            self._index_num, 
            pkl, 
            self.index.ntotal, 
            self.index.d, 
            self._data_version)
        self._saved_data_version = self._data_version
        self.substatus = IndexSubStatus.READY
        _logger.info(f"Done saving index #{self._index_num}.")

    def update(self) -> UpdateResult:
        if (self.status != IndexStatus.TRAINED):
            return UpdateResult.INDEX_NOT_READY

        #print(f"Checking for changes from version {from_version}...")
        #version, type, reason, changes = mf.update_index_with_ct(self._data_version)    

        change_info = self._db.get_changes(self._data_version)    
        version = int(change_info["Metadata"]["Sync"]["Version"])
        type = change_info["Metadata"]["Sync"]["Type"]
        reason = change_info["Metadata"]["Sync"]["ReasonCode"]
        changes = change_info.get("Data")
            
        if (type == "Diff"):        
            _logger.info(f"Found new version. Updating index to {version}...")        
            for c in changes:
                id = int(c["id"])            
                operation = c["$operation"]            
                _logger.debug(f"Id={id}, Op={operation}")
                match operation:
                    case "I":
                        vector = json.loads(c["vector"])
                        self.index.add_with_ids(np.asarray([vector]), np.asarray([id]))
                    case "D":
                        self.index.remove_ids(np.asarray([id]))
                    case "U":
                        vector = json.loads(c["vector"])
                        self.index.remove_ids(np.asarray([id]))
                        self.index.add_with_ids(np.asarray([vector]), np.asarray([id]))
                    case _:
                        raise Exception(f"Unknown operation: {operation}")

            self._data_version = version
            _logger.info(f"Done. New version is {version}.")     
            return UpdateResult.DONE
        else:
            if (reason != 1):
                match reason:
                    case 2:
                        return UpdateResult.INDEX_IS_STALE
                    case _:
                        return UpdateResult.UNKNOWN
            else:
                return UpdateResult.NO_CHANGES


    def query(self, vector:list[float], limit:int):
        qv = np.asarray([vector])
        dist, ids = self.index.search(qv, limit)
        r = dict(zip([int(i) for i in ids[0]], 1-dist[0]))
        return {"result": json.loads(json.dumps(r, cls=NpEncoder))}

    def get_status(self):
        if (self.index):
            return {
                "id": self._index_num,
                "type": type(self.index).__name__,
                "status": self.status,
                "substatus": self.substatus,
                "data_version": self._data_version,
                "saved_data_version": self._saved_data_version,
                "dimensions": self.index.d,
                "vectors": self.index.ntotal
            }
        else:
            return {
                "id": self._index_num,
                "status": self.status,
                "substatus": self.substatus,
                "data_version": self._data_version
            }
    
    def get_version(self):
        return self._data_version