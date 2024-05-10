import os
import logging
import json

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, status as HTTPStatus, Response
from contextlib import asynccontextmanager

from db.index import NoIndex
from db.utils import DataSourceConfig
from db.faiss import FaissIndex, IndexStatus, UpdateResult
from internals import State, Vector, ConfigParser, IndexRequest

from config import BACKGROUND_JOBS

load_dotenv()

api_version = "0.0.3"

_logger = logging.getLogger("uvicorn")

state = State()

@asynccontextmanager
async def lifespan(app: FastAPI):
    _config_parser = ConfigParser(BACKGROUND_JOBS)
    _change_tracking_trigger = _config_parser.get_cron_trigger("CHANGE_TRACKING_CRONTAB", "*/1 * * * * *")
    _save_index_trigger = _config_parser.get_cron_trigger("SAVE_INDEX_CRONTAB", "* */1 * * * *")

    _logger.info("Change Tracking Trigger Schedule: " + str(_change_tracking_trigger))
    _logger.info("Save Index Schedule: " + str(_save_index_trigger))

    scheduler = state.get_scheduler()
    #scheduler.add_job(change_tracking_monitor, _change_tracking_trigger, id='change_monitor', coalesce=True)
    #cheduler.add_job(save_index, _save_index_trigger, id='save_index', coalesce=True)
    #scheduler.add_job(bootstrap, id="bootstrap")
    scheduler.start()    
    _logger.info("Starting API...")
    yield
    _logger.info("Closing API...")
    state.clear()

api = FastAPI(lifespan=lifespan)

def assert_index_is_ready():    
    if (state.index.status != IndexStatus.TRAINED):
        raise HTTPException(
            status_code = HTTPStatus.HTTP_400_BAD_REQUEST, 
            detail = state.index.get_status()
        )

def bootstrap():
    _logger.info("Bootstrapping...")
    state.database_engine.initalize()
    _logger.info("Bootstrap complete.")

def save_index():
    if (state.index.status != IndexStatus.TRAINED):
        return
    
    s = state.get_scheduler()
    s.pause_job("save_index")

    state.index.save()

    s.resume_job("save_index")

def change_tracking_monitor():
    if (state.index.status != IndexStatus.TRAINED):
        return

    s = state.get_scheduler()
    s.pause_job("change_monitor")

    ur = state.index.update()

    match ur:
        case UpdateResult.NO_CHANGES:  
            s.resume_job("change_monitor")
        case UpdateResult.DONE:  
            s.resume_job("change_monitor")
        case UpdateResult.INDEX_IS_STALE:
            print(f"No changes found as index is stale. Full rebuild is needed.")
            print(f"Change detection is stopped.")
            s.remove_job("change_monitor")       
        case UpdateResult.UNKNOWN:
            print(f"No changes found. Reason unknown.")
            print(f"Change detection is stopped.")
            s.remove_job("change_monitor")       

@api.get("/")
def welcome():
    return {
            "server": state.get_status(),
            "version": api_version
            }

# @api.post("/faiss/build")
# def build(tasks: BackgroundTasks, indexRequest: IndexRequest, force: bool = False):    
#     if (isinstance(state.index, NoIndex)):
#         _logger.info("No index found, creating FAISS index...")
#         state.index = FaissIndex(state.database_engine)

#     tasks.add_task(state.index.create)    
#     return Response(status_code=202)     


@api.post("/faiss/build")
def build(tasks: BackgroundTasks, indexRequest: IndexRequest, force: bool = False): 
    if (isinstance(state.index, NoIndex) == False):        
        raise HTTPException(detail=f"An index (#{state.index.id}) is already being built.", status_code=500)
      
    config = DataSourceConfig()
    config.source_table_schema = indexRequest.table.table_schema
    config.source_table_name = indexRequest.table.table_name
    config.source_id_column_name = indexRequest.column.id
    config.source_vector_column_name = indexRequest.column.vector
    config.vector_dimensions = indexRequest.vector.dimensions
  
    id = None
    try:
        state.set_status("initializing")
        state.index = FaissIndex.from_config(config)
        id = state.index.initialize_build(force)
    except Exception as e:
        _logger.error(f"Error during initialization: {e}")
        state.set_status("error during initialization: " + str(e))
        state.clear()
        raise HTTPException(detail=str(e), status_code=500)

    tasks.add_task(_internal_build) 

    r = {
            "id": int(id),
            "status": state.get_status()
        }
    j = json.dumps(r, default=str)

    return Response(content=j, status_code=202, media_type='application/json')

@api.post("/faiss/query")
def faiss_query(query: Vector):
    assert_index_is_ready()
    return state.index.query(query.vector, 10)

# @app.post("/index/faiss/add", status_code=202)
# def faiss_add(query: Vector):
#     assert_index_is_ready()
#     index:faiss.IndexFlat = old_state["index:faiss"]  
#     index.add_with_ids(np.asarray([query.vector]), np.asarray([query.id]))
#     return get_index_status()

@api.post("/faiss/load")
def faiss_create(tasks: BackgroundTasks):      
    if (isinstance(state.index, NoIndex)):
        _logger.info("No index found, loading FAISS index...")
        state.index = FaissIndex(state.database_engine)

    tasks.add_task(state.index.load)    
    return Response(status_code=202)

@api.post("/faiss/save")
def faiss_create(tasks: BackgroundTasks):    
    assert_index_is_ready()
    tasks.add_task(state.index.save)    
    return Response(status_code=202)

# @api.get("/faiss/info")
# def faiss_info():
#     return {
#         "state": state.index.get_status()
#     }

def _internal_build():
    try:
        state.set_status("building")
        state.index.build()
    except Exception as e:
        _logger.error(f"Error building index: {e}")
        state.set_status("error during index build: " + str(e))
    finally:
        state.clear()