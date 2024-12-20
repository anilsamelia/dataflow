import abc

from modules.bigquery.bigquery import BigqueryClient
from modules.config.config import SyncConfig


class BaseABC(metaclass=abc.ABCMeta):

    def __init__(self, bigquery_client: BigqueryClient, conf: SyncConfig):
        self.client = bigquery_client
        self.conf = conf
        self.encrypt_fn = conf.encrypt_function

    def sync(self, source_table, schema):
        pass

    def has_drifted(self, source_table, schema):
        return True
