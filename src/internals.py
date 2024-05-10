from pydantic import BaseModel, Field
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from db.database import DatabaseEngine
from db.index import NoIndex

from config import BACKGROUND_JOBS

class TableInfo(BaseModel):
    table_schema: str = Field(alias="schema")
    table_name: str = Field(alias="name")

class ColumnInfo(BaseModel):
    id: str 
    vector: str 

class VectorInfo(BaseModel):
    dimensions: int

class IndexRequest(BaseModel):
    table: TableInfo 
    column: ColumnInfo
    vector: VectorInfo

class Vector(BaseModel):
    id: int = None
    vector: list[float] = []

class State:
    def __init__(self) -> None:
        self._scheduler = BackgroundScheduler()
        self.index = NoIndex()
        self.current_status = "idle"
        self.last_status = "idle"

    def get_scheduler(self) -> BackgroundScheduler:
        return self._scheduler
    
    def set_status(self, status:str):   
        self.last_status = self.current_status
        self.current_status = status
    
    def get_status(self)->str:
        return {
            "status": {
                "current": self.current_status,
                "last": self.last_status
            },
            "index_id": self.index.id
        }  

    def clear(self):
        self._scheduler.shutdown()
        self._scheduler = None
        self.last_status = self.current_status
        self.current_status = "idle"
        self.index = NoIndex()

class ConfigParser:
    def __init__(self, configuration) -> None:
        self._configuration = configuration

    def get_cron_trigger(self, property_name, default_value) -> CronTrigger:
        cron_exp = self._configuration[property_name] or default_value
        cron_items = cron_exp.split(" ")
        if (len(cron_items) != 6):
            raise Exception("crontab expression must have 6 values (sec min hour day_of_month month day_of_week).")

        return CronTrigger(
            second=cron_items[0],
            minute=cron_items[1],
            hour=cron_items[2],
            day=cron_items[3],
            month=cron_items[4],
            day_of_week=cron_items[5]
        )