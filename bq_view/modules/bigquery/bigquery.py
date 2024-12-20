from typing import Union, Callable

from google.api_core import exceptions
from google.cloud import bigquery
from google.cloud.bigquery.table import TableListItem
from google.oauth2 import service_account

from modules.config.config import SyncConfig


class BigqueryClient:

    def __init__(self, project_id: str, conf: SyncConfig):
        credentials = service_account.Credentials.from_service_account_file(conf.service_account,
            scopes=["https://www.googleapis.com/auth/cloud-platform"])
        self.client = bigquery.Client(credentials=credentials,project=project_id)
        self.project_id = project_id
        self.conf = conf

    '''Generate a dataset if it doesn't already exist and provide the dataset as output'''
    def create_dataset(self, dataset):
        dataset = dataset if not self.conf.destination_dataset_prefix else f'{self.conf.destination_dataset_prefix}_{dataset}'
        dataset = bigquery.Dataset(f'{self.project_id}.{dataset.upper()}')
        #print(f"Creating dataset: {dataset.dataset_id}")
        self.client.create_dataset(dataset, timeout=30, exists_ok=True)
        return dataset

    ''' Request for creating or replacing a VIEW in the utility query.'''
    def create_or_replace_view_as(self, source_table: str, destination_table: str, select_fn: Callable = None,
                                  except_fn: Callable = None, from_fn: Callable = None, where_fn: Callable = None):
        destination_table = destination_table if not self.conf.destination_dataset_prefix else f'{self.conf.destination_dataset_prefix}_{destination_table}'
        sql = f"CREATE OR REPLACE VIEW {destination_table} AS"

        select_args = select_fn() if select_fn is not None else None
        except_args = except_fn() if except_fn is not None else None
        from_args = from_fn() if from_fn is not None else None
        where_args = where_fn() if where_fn is not None else None

        sql += f" SELECT {', '.join(select_args)}" if select_args else " SELECT *"
        sql += f" EXCEPT ({', '.join(except_args)})" if except_args else ""
        sql += f" FROM {', '.join(from_args)}" if from_args else f" FROM {source_table}"
        sql += f" WHERE {' AND '.join(where_args)}" if where_args else ""

        return self.query(sql)

    ''' Execute the query '''
    def query(self, query):
        job = self.client.query(query)
        return job.result()

    ''' GET List of Tables from dataset'''
    def list_tables(self, dataset):
        try:
            dataset = bigquery.Dataset(f'{self.project_id}.{dataset}')
            return list(map(self.get_table, list(self.client.list_tables(dataset))))
        except exceptions.NotFound:
            return []

    def get_table(self, table: Union[TableListItem, str]):
        return self.client.get_table(table)

    '''Ensure the existence of the table; if it does not exist, return None.'''
    def table_exist(self, table: Union[TableListItem, str]):
        try:
            return self.get_table(table)
        except Exception as e:
            return None

