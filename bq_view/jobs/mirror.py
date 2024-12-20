import abc
from http.client import NOT_FOUND

from jobs._base import BaseABC
from modules.config.config import get_encryption_fields, get_table_name_by_strategy
from modules.util.datastream_utility import checkTableName


class MirrorSchemaJob(BaseABC, metaclass=abc.ABCMeta):

    def sync(self, source_table, schema):
        destination_dataset = f"{schema.upper()}_MIRROR"
        created_dataset = self.client.create_dataset(destination_dataset)
        destination_table_id=get_table_name_by_strategy(self.conf,source_table.table_id,schema)

        if self.has_drifted(source_table, created_dataset,destination_table_id):
            aggregates = ['datastream_metadata.source_timestamp op_event_dtm_epoch', '*']
            data_rang_col = checkTableName(destination_table_id)
            if data_rang_col is not None:
                aggregates.insert(1,data_rang_col)

            cols = [s.name for s in source_table.schema]
            encrypt_cols = list(set(cols).intersection(get_encryption_fields(self.conf, destination_table_id)))
            wrapped_encrypted_cols = list(map(lambda c: f'`{self.encrypt_fn}`(CAST({c} as STRING)) AS {c}', encrypt_cols))
            self.client.create_or_replace_view_as(
                source_table=f"{source_table.dataset_id}.{source_table.table_id}",
                destination_table=f"{destination_dataset}.{destination_table_id}",
                select_fn=lambda: wrapped_encrypted_cols + aggregates,
                except_fn=lambda: encrypt_cols + ['datastream_metadata'],
            )
            return True
        else:
            return False

    '''Check:If any encrypted columns are added or removed in the configuration, it will return true; otherwise,
    it will return false. Additionally, if the VIEW does not exist, it will also return true'''
    def has_drifted(self, source_table, destination_dataset,destination_table_id):
        dest_table = self.client.table_exist(f"{destination_dataset.dataset_id}.{destination_table_id}")
        if dest_table is not None:
            destination_schema = dest_table.schema
            config_encrypt_cols = get_encryption_fields(self.conf, source_table.table_id)
            unencrypted_schema=list(filter(lambda elm: (elm.field_type != 'BYTES'), destination_schema))
            unencrypted_cols =[s.name for s in unencrypted_schema]
            common_unencrypt_cols =set(unencrypted_cols).intersection(config_encrypt_cols)
            encrypted_schema=list(filter(lambda elm: (elm.field_type == 'BYTES'), destination_schema))
            encrypted_cols =[s.name for s in encrypted_schema]
            common_encrypted_cols = set(encrypted_cols).difference(config_encrypt_cols)
            if common_unencrypt_cols or common_encrypted_cols:
                return True
            else:
                return False
        else:
            return True
            