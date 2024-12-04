from batch_manager import BatchManager
from config import Config
from forced_prefetching import ForcedPrefetching



def sync(config: Config):
    if config.output.force_prefetching:
        ForcedPrefetching(config=config).run()
    batch_manager = BatchManager(config=config)
    batch_size, sync_length, max_filter_field = batch_manager.run()
    print(f"start sync for {config.output.table_name}")
    for offset in sync_length:
        data = config.input.get_data(offset=offset, limit=batch_size, updated_at=max_filter_field)
        if not data:
            print(f"table {config.output.table_name} synchronized")
            break
        if config.transform is not None:
            data = config.transform.run(data)
        config.output.set_data(data)
        print(f"offset {offset}")
