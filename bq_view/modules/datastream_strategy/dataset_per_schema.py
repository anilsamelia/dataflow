from modules.bigquery.bigquery import BigqueryClient
from modules.config.config import SyncConfig


def list_tables(bigquery_client: BigqueryClient, config: SyncConfig):
    # Iterate over each schema config
    for schema in config.schemas:
        tables = bigquery_client.list_tables(dataset=f"{config.source_dataset_prefix}_{schema}")

        for table in tables:
            yield schema, table
