from dataclasses import dataclass

from config import Config


@dataclass
class BatchManager:
    batch_size = 10000
    config: Config

    def run(self):
        max_filter_field = self.get_max_filter_field()
        sync_length = range(self.get_sync_length(max_filter_field) // self.batch_size + 1)
        print(f"Size of sync is no more than {len(sync_length) * self.batch_size}"
              f" rows for {self.config.output.table_name}")
        return self.batch_size, sync_length, max_filter_field

    def get_max_filter_field(self):
        print("Get max filter field value")
        return self.config.output.filter_field_value()

    def get_sync_length(self, max_filter_field):
        print("Get count of rows")
        return self.config.input.get_sync_length(max_filter_field)