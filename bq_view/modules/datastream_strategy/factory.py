from modules.config.config import SyncConfig
from modules.datastream_strategy import dataset_per_schema, dataset_single


def create_instance(config: SyncConfig):
    if config.strategy == 'DATASET_PER_SCHEMA':
        return dataset_per_schema
    elif config.strategy == 'DATASET_SINGLE':
        return dataset_single
    else:
        raise Exception
