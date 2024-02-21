from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from sqlext.database import DatabaseEngine
from sqlext.index import NoIndex

from config import INDEX, BACKGROUND_JOBS

class Vector(BaseModel):
    id: int = None
    vector: list[float] = []

class State:
    def __init__(self) -> None:
        self._scheduler = BackgroundScheduler()
        self.database_engine = DatabaseEngine(INDEX)
        self.index = NoIndex()
        pass

    def get_scheduler(self) -> BackgroundScheduler:
        return self._scheduler
    
    def clear(self):
        self._scheduler.shutdown()
        self._scheduler = None
        self.database_engine = None
        self.index = None

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