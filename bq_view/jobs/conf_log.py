import abc
from http.client import NOT_FOUND

from jobs._base import BaseABC
from modules.config.config import get_encryption_fields, get_table_name_by_strategy



class ConfLogSchemaJob(BaseABC, metaclass=abc.ABCMeta):

    def sync(self, source_table, schema):
        destination_dataset = f"{schema.upper()}_CONF_LOG"
        created_dataset = self.client.create_dataset(destination_dataset)
        destination_table_id=get_table_name_by_strategy(self.conf,source_table.table_id,schema)

        if self.has_drifted(source_table, created_dataset,destination_table_id):
            source_schema =  self.client.get_table(source_table).schema
            cols = [s.name for s in source_table.schema]
            # No encrypt cols for CONF_LOG
            encrypt_cols = []
            raw_cols = list(filter(lambda c: c not in encrypt_cols and c != 'datastream_metadata', cols))

            wrapped_encrypted_cols = list(
                map(lambda c: f"`{self.encrypt_fn}`(CAST(JSON_VALUE(payload, '$.{c}') as STRING)) AS {c}",
                    encrypt_cols))
            #wrapped_raw_cols = list(map(lambda c: f"JSON_VALUE(payload, '$.{c}') AS {c}", raw_cols))
            wrapped_raw_cols=self.get_wrapped_raw_cols(source_schema,raw_cols)

            other_cols = [
                          'DATE(source_timestamp) as op_event_date',
                          'read_timestamp as op_log_created_dtm',
                          'source_metadata.change_type as op_transaction_type',
                          'source_metadata.tx_id as op_mysql_transaction_id',
                          'source_timestamp as op_event_dtm_epoch',
                          'read_timestamp as op_processed_dtm'
                          ]

            data_rang_col = checkTableName(destination_table_id)
            data_rang_val = []
            if data_rang_col != None:
                data_rang_val.append(data_rang_col)

            self.client.create_or_replace_view_as(
                source_table=self.conf.log_staging_table,
                destination_table=f"{destination_dataset}.{destination_table_id}",
                select_fn=lambda: wrapped_encrypted_cols + wrapped_raw_cols + data_rang_val + other_cols,
                from_fn=lambda: [self.conf.log_staging_table, "unnest(source_metadata) as source_metadata"],
                where_fn=lambda: [f"object = '{schema}_{source_table.table_id}'"]
            )
    '''Check: If any unencrypted are added in staging table, it will return true; otherwise,
    it will return false. Additionally, if the VIEW does not exist, it will also return true'''
    def has_drifted(self, source_table, destination_dataset,destination_table_id):
        dest_table = self.client.table_exist(f"{destination_dataset.dataset_id}.{destination_table_id}")
        if dest_table is not None:
            destination_table_id=f"{destination_dataset.dataset_id}.{destination_table_id}"
            destination_schema = self.client.get_table(destination_table_id).schema
            destination_filter_schema=list(filter(lambda elm: (elm.name != 'op_processed_dtm' and elm.name != 'op_event_dtm_epoch' and elm.name != 'op_transaction_type' and elm.name != 'op_log_created_dtm' and elm.name != 'op_event_date' and elm.name != 'op_mysql_transaction_id'), destination_schema))
            source_schema =  self.client.get_table(source_table).schema
            source_filter_schema = list(filter(lambda elm: (elm.name != 'datastream_metadata'),source_schema))
            source_cols= [s.name for s in source_filter_schema]
            destination_cols = [s.name for s in destination_filter_schema]
            # Identify the differing columns between the stage_table and the columns in the log_table.
            diff_cols =set(source_cols).difference(destination_cols)
            if not diff_cols:
                return False
            else:
                return True
        else:
            return True

    def get_wrapped_raw_cols(self,table_schema, raw_cols):
        cols_list = []
        for field in table_schema:
            if field.name in raw_cols:
                col_type = field.field_type
                if field.field_type == 'FLOAT':
                    col_type ='FLOAT64'
                if field.field_type == 'JSON':
                     col= "JSON_ARRAY(JSON_VALUE(payload, '$.{col_name}')) AS {col_name}".format(col_name = field.name)
                else:
                    col= "Cast(JSON_VALUE(payload, '$.{col_name}') AS {field_type}) AS {col_name}".format(col_name = field.name, field_type = col_type)
                cols_list.append(col)
        return cols_list

def checkTableName(destination_table_id):
    table = 'service_provider_unavailability_period'
    if table == destination_table_id:
        return 'CONCAT("[",CAST(JSON_VALUE(payload, "$.unavailability_period_start") AS DATE),"," ,DATE_ADD(CAST(JSON_VALUE(payload, "$.unavailability_period_end") AS DATE), INTERVAL 1 DAY),")") unavailability_period_range'
    else:
        return None
