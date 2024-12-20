import abc

from google.cloud.bigquery import Table

from jobs._base import BaseABC
from modules.config.config import get_table_name_by_strategy
from modules.util.datastream_utility import checkTableName


class ConfSchemaJob(BaseABC, metaclass=abc.ABCMeta):

    def sync(self, source_table: Table, schema):
        destination_dataset = f"{schema.upper()}_CONF"
        created_dataset=self.client.create_dataset(destination_dataset)
        destination_table_id=get_table_name_by_strategy(self.conf,source_table.table_id,schema)

        if self.has_drifted(source_table, created_dataset,destination_table_id):
            aggregates = ['datastream_metadata.source_timestamp op_event_dtm_epoch', '*']
            data_rang_col = checkTableName(destination_table_id)
            if data_rang_col is not None:
                aggregates.insert(1,data_rang_col)
            return self.client.create_or_replace_view_as(
                source_table=f"{source_table.dataset_id}.{source_table.table_id}",
                destination_table=f"{destination_dataset}.{destination_table_id}",
                select_fn=lambda: aggregates,
                except_fn=lambda: ['datastream_metadata'],
            )
    
    '''Check:if the view exist return False; else return true. There's no requirement to compare regular columns and encrypted columns; we're employing the '*' wildcard for all columns.'''
    def has_drifted(self,source_table,destination_dataset,destination_table_id):
        return False if self.client.table_exist(f"{destination_dataset.dataset_id}.{destination_table_id}") is not None else True
