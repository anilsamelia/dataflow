from modules.bigquery.bigquery import BigqueryClient
from modules.config.config import SyncConfig


def list_tables(bigquery_client: BigqueryClient, config: SyncConfig):
    tables = bigquery_client.list_tables(dataset=config.source_dataset)

    # Iterate over each schema config
    for schema in config.schemas:
        # Datastream creates tables in schema_table format
        # Filter tables that starts with this schema
        schema_tables, tables = pop_matches(tables, schema)

        for table in schema_tables:
            yield schema, table


def pop_matches(tables, schema):
    match, not_match = [], []
    for t in tables:
        match.append(t) if t.table_id.startswith(f'{schema}_') else not_match.append(t)
    return match, not_match
