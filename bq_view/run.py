from jsonargparse import CLI

from jobs.conf import ConfSchemaJob
from jobs.log import LogSchemaJob
from jobs.mirror import MirrorSchemaJob
from jobs.conf_log import ConfLogSchemaJob
from modules.bigquery.bigquery import BigqueryClient
from modules.config.config import SyncConfig
from modules.datastream_strategy.factory import create_instance


def main():
    config = CLI(SyncConfig, as_positional=False)

    bigquery_client = BigqueryClient(project_id=config.project_id, conf=config)
    mirror_schema_job = MirrorSchemaJob(bigquery_client, config) if config.create_mirror_views else None
    conf_schema_job = ConfSchemaJob(bigquery_client, config) if config.create_conf_views else None
    log_schema_job = LogSchemaJob(bigquery_client, config) if config.create_log_views else None
    conf_log_schema_job = ConfLogSchemaJob(bigquery_client,config) if config.create_conf_log_views else None

    ''' Iterate all the tables with schema name'''
    for schema, table in create_instance(config).list_tables(bigquery_client, config):
        print(f'Running for {schema} - {table.table_id}')

        # Create Unencrypted View for the table - CONF Schema
        conf_schema_job.sync(source_table=table, schema=schema) if conf_schema_job is not None else None

        # Create CONF_LOG_Schema
        if schema in config.conf_log_schemas:
            conf_log_schema_job.sync(source_table=table, schema=schema) if conf_log_schema_job is not None else None

        # Create Encrypted View for the table - MIRROR Schema
        is_recreated = mirror_schema_job.sync(source_table=table, schema=schema) if mirror_schema_job is not None else None

        # Create Encrypted Log View - LOG Schema
        log_schema_job.sync(source_table=table, schema=schema,is_recreated=is_recreated) if log_schema_job is not None else None



if __name__ == "__main__":
    main()

