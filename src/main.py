import os
import logging

from dotenv import load_dotenv
from fastapi import BackgroundTasks, FastAPI, HTTPException, status as HTTPStatus, Response
from contextlib import asynccontextmanager
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler

from sqlext.index import NoIndex
from sqlext.database import DatabaseEngine
from sqlext.faiss import FaissIndex, IndexStatus, UpdateResult

from internals import State, Vector, ConfigParser

from config import INDEX, BACKGROUND_JOBS

load_dotenv()

_logger = logging.getLogger("uvicorn")

index_num = os.environ["DEFAULT_INDEX_MODEL_ID"] or 1
api_version = "0.0.3"

state = State()

@asynccontextmanager
async def lifespan(app: FastAPI):
    _config_parser = ConfigParser(BACKGROUND_JOBS)
    _change_tracking_trigger = _config_parser.get_cron_trigger("CHANGE_TRACKING_CRONTAB", "*/1 * * * * *")
    _save_index_trigger = _config_parser.get_cron_trigger("SAVE_INDEX_CRONTAB", "* */1 * * * *")

    _logger.info("Change Tracking Trigger Schedule: " + str(_change_tracking_trigger))
    _logger.info("Save Index Schedule: " + str(_save_index_trigger))

    scheduler = state.get_scheduler()
    scheduler.add_job(change_tracking_monitor, _change_tracking_trigger, id='change_monitor', coalesce=True)
    scheduler.add_job(save_index, _save_index_trigger, id='save_index', coalesce=True)
    scheduler.add_job(bootstrap, id="bootstrap")
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
            "server": "running",
            "version": api_version
            }

@api.post("/index/faiss/create")
def faiss_create(tasks: BackgroundTasks):    
    if (isinstance(state.index, NoIndex)):
        _logger.info("No index found, creating FAISS index...")
        state.index = FaissIndex(state.database_engine)

    tasks.add_task(state.index.create)    
    return Response(status_code=202)     

@api.post("/index/faiss/query")
def faiss_query(query: Vector):
    assert_index_is_ready()
    return state.index.query(query.vector, 10)

# @app.post("/index/faiss/add", status_code=202)
# def faiss_add(query: Vector):
#     assert_index_is_ready()
#     index:faiss.IndexFlat = old_state["index:faiss"]  
#     index.add_with_ids(np.asarray([query.vector]), np.asarray([query.id]))
#     return get_index_status()

@api.post("/index/faiss/load")
def faiss_create(tasks: BackgroundTasks):      
    if (isinstance(state.index, NoIndex)):
        _logger.info("No index found, loading FAISS index...")
        state.index = FaissIndex(state.database_engine)

    tasks.add_task(state.index.load)    
    return Response(status_code=202)

@api.post("/index/faiss/save")
def faiss_create(tasks: BackgroundTasks):    
    assert_index_is_ready()
    tasks.add_task(state.index.save)    
    return Response(status_code=202)

@api.get("/index/faiss/info")
def faiss_info():
    return {
        "state": state.index.get_status()
    }