import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Union, Optional
from unittest.mock import Mock

from sqlalchemy import Table
from sqlalchemy.orm import Session

import psycopg2


class SQLAlchemyConnectionRetryConfig:
    DELAY_BETWEEN_ATTEMPTS_SEC = 5
    NUM_OF_TRIES = 3
    TRIGGER_EXCEPTIONS = (
        psycopg2.OperationalError
    )


@dataclass
class SynchronizationState:
    session_uid: uuid.UUID
    log_session: Session = None
    table_name: Union[Table, Mock] = Mock(__tablename__='undefined_table')
    source_table: Table = None
    message: str = 'Empty message'
    verbose: bool = False
    batch_size: int = 10000
    dwh_session: Session = None
    lock_session: Session = None
    refresh_mat_views_session: Session = None
    source_engine_type: str = str()
    opensearch_scroll_size: str = '25m'
    reconciliation_interval_days: int = 30
    fix: bool = False
    prod_number: Union[str, int] = None
    sync_name: str = str()
    postgres_statement_timeout: int = None
    schema: Optional[str] = None
    mongo_batch_size = 3000

    def __post_init__(self):
        self.sync_name = self.sync_name.split('/')[-1]


class ProdNumber(Enum):
    prod01 = 1
    prod02 = 2
    prod03 = 3
    prod04 = 4


class EngineType(Enum):
    mysql = 'mysql+mysqlconnector'
    postgres = 'postgresql'
    postgres_retry = 'postgresql+retry'
    clickhouse = 'clickhouse'


AIRFLOW_SPARK_DRIVER_PATH = '/usr/local/airflow/dags/drivers/*'
