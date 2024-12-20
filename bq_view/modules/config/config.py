from dataclasses import dataclass

from jsonargparse import set_docstring_parse_options
from jsonargparse.typing import extend_base_type

set_docstring_parse_options(attribute_docstrings=True)


def list_sort_desc(type_class, value):
    return value.sort(reverse=True)


SortedDescList = extend_base_type("SortedDescList", list, list_sort_desc)


@dataclass
class SyncConfig:
    """ GCP Project ID """
    project_id: str

    """ List of schemas to process """
    schemas: SortedDescList

    """ List of CONF_LOG schema to process """
    conf_log_schemas: SortedDescList

    """ The encryption function to encrypt columns in format dataset.function_name. """
    encrypt_function: str

    """ The staging table that points to sync log files  in format dataset.table_name """
    log_staging_table: str

    """ List of fields that needs to be encrypted """
    encryption_global_fields: list

    """ List of fields that needs to be encrypted grouped by table """
    encryption_table_fields: dict

    """ Enable creation of LOG schema (encrypted) views """
    create_log_views: bool = True

    """ Enable creation of MIRROR schema (encrypted) views """
    create_mirror_views: bool = True

    """ Enable creation of CONF schema (non-encrypted) views """
    create_conf_views: bool = True

    """ Enable creation of CONF_LOG schema views """
    create_conf_log_views: bool = True

    """ Prefix to add to any dataset that are created """
    destination_dataset_prefix: str = None

    """ Datastream Strategy: DATASET_PER_SCHEMA / DATASET_SINGLE"""
    strategy: str = 'DATASET_PER_SCHEMA'

    """ [Strategy: DATASET_SINGLE] Source dataset synced through datastream """
    source_dataset: str = None

    """ [Strategy: DATASET_PER_SCHEMA] Prefix that's added by datastream to dataset """
    source_dataset_prefix: str = None

    """ Service account """
    service_account: str = None

''' Get encrypted fields from configuration'''
def get_encryption_fields(config: SyncConfig, table: str):
    return config.encryption_global_fields + (
        config.encryption_table_fields[table] if table in config.encryption_table_fields else [])

''' Retrieve the table name based on the configured strategy type for the single dataset Change Data Capture (CDC),
    and generate the staging table name as SCHEMANAME_TABLENAME.'''
def get_table_name_by_strategy(config: SyncConfig, table: str,schema: str):
    if config.strategy == 'DATASET_SINGLE':
        split_index = len(schema)+1
        return table[split_index:]
    return table