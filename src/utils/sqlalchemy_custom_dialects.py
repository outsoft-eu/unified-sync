from abc import ABC, abstractmethod

from retry import retry
from sqlalchemy.dialects.postgresql.psycopg2 import PGDialect_psycopg2

from src.utils.constants import SQLAlchemyConnectionRetryConfig as rc


class CustomDialect(ABC):
    @property
    @abstractmethod
    def reg_name(self):
        pass


class DialectPGRetry(PGDialect_psycopg2, CustomDialect):
    reg_name = "postgresql.retry"

    @retry(exceptions=rc.TRIGGER_EXCEPTIONS,
           tries=rc.NUM_OF_TRIES,
           delay=rc.DELAY_BETWEEN_ATTEMPTS_SEC)
           # logger=logger)
    def connect(self, *args, **kwargs):
        return super().connect(*args, **kwargs)
