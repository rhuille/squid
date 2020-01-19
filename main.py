
import importlib
from typing import Dict

from pandas import DataFrame, read_sql
from toucan_connectors import (
    CONNECTORS_CATALOGUE, ToucanConnector)
from sqlalchemy import create_engine


class ToucanConnectorsExecuter():
    def __init__(self, configuration):
        connectors_conf = configuration['CONNECTORS']
        ds_conf = configuration['DATA_SOURCES']
        self.connectors_conf = dict([(c['name'], c) for c in connectors_conf])
        self.data_sources_conf = dict([(d['domain'], d) for d in ds_conf])
        self.domains = [d['domain'] for d in ds_conf]
        self.connectors = [c['name'] for c in connectors_conf]

    def _get_connector_model(self, name: str) -> ToucanConnector:
        connector_conf = self.connectors_conf[name]
        path_to_module, connector_class_name = (
            CONNECTORS_CATALOGUE[connector_conf['type']].rsplit('.', 1)
        )
        connector_module = importlib.import_module(
            f".{path_to_module}",
            package="toucan_connectors"
        )
        return getattr(connector_module, connector_class_name)

    def get_df(self, domain: str) -> DataFrame:
        data_source_conf = self.data_sources_conf[domain]
        connector_conf = self.connectors_conf[data_source_conf['name']]
        #
        connector_model = self._get_connector_model(connector_conf['name'])
        data_source_model = connector_model.data_source_model
        #
        connector = connector_model(**connector_conf)
        data_source = data_source_model(**data_source_conf)
        return connector.get_df(data_source)

    def get_dfs(self) -> Dict[str, DataFrame]:
        dfs = {}
        for domain in self.domains:
            dfs[domain] = self.get_df(domain)
        return dfs


class SQLAlchemyExecuter():
    languages = "SQL"

    def __init__(self, store: Dict[str, DataFrame]):
        self.engine = create_engine('sqlite:///:memory:', echo=False)
        for df_name, df in store.items():
            # df_name is a str, df is a DataFrame
            df[df.columns] = df[df.columns].astype(str)  # TODO: remove this line
            df.to_sql(df_name, con=self.engine, index=False)

    def execute(self, query: str, output_name: str):
        self.engine.execute(f"create table {output_name} as {query}")

    def get(self, table: str) -> DataFrame:
        return read_sql(f"select * from {table}", self.engine)
