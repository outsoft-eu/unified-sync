from dataclasses import dataclass
import logging

from airflow_logging.logging_classes import LoggerName
from config import Config

logger = logging.getLogger(LoggerName.AIRFLOW_TASK.value)


@dataclass
class ForcedPrefetching:
    config: Config

    def run(self):
        logger.info("We need to prefetch this table")
        mismatch_bunch = self.get_mismatch_bunch()
        logger.info(f"Number of mismatches: {len(mismatch_bunch)}")
        deleted_rows = self.config.output.delete_prefetch_rows(mismatch_bunch)
        print(f"Deleted {deleted_rows} mismatched rows")

    def get_mismatch_bunch(self):
        input_rows = set(self.config.input._prefetch_rows())
        output_rows = set(self.config.output._prefetch_rows())

        mismatch_bunch = output_rows - input_rows
        return mismatch_bunch
