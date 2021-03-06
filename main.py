
import importlib
from typing import Dict
import json

from pandas import DataFrame, read_sql
from toucan_connectors import (
    CONNECTORS_CATALOGUE, ToucanConnector)
from sqlalchemy import create_engine
from rpy2 import robjects  # needs to install r: `brew install r` for instance
from rpy2.robjects import pandas2ri
from pymongo import MongoClient


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


# Executer: class with the following methods:
# __init__(store: Dict[str, DataFrame]) -> Executer
# execute(query: str, output_name: str) -> Executer
# get(table: str) -> DataFrame
# TODO: create a generic parent class


class MongoExecuter():
    def __init__(self, store: Dict[str, DataFrame]):
        self._db = MongoClient().db
        for df_name, df in store.items():
            # df_name is a str, df is a DataFrame
            self._db[df_name].insert_many(df.to_dict('records'))

    def execute(self, query: str, output_name: str, input_name: str):
        query_json = json.loads(query) + [{"$out": output_name}]
        self._db[input_name].aggregate(query_json)
        return self

    def get(self, table: str) -> DataFrame:
        return DataFrame.from_records(
            self._db[table].find(projection={"_id": False})
        )


class SQLAlchemyExecuter():
    language = "SQL"

    def __init__(self, store: Dict[str, DataFrame]):
        self._engine = create_engine('sqlite:///:memory:', echo=False)
        for df_name, df in store.items():
            # df_name is a str, df is a DataFrame
            df[df.columns] = df[df.columns].astype(str)  # TODO: remove this line
            df.to_sql(df_name, con=self._engine, index=False)

    def execute(self, query: str, output_name: str):
        self._engine.execute(f"create table {output_name} as {query}")
        return self

    def get(self, table: str) -> DataFrame:
        return read_sql(f"select * from {table}", self._engine)


class PandasExecuter():
    language = "pandas"

    def __init__(self, store: Dict[str, DataFrame]):
        self._dfs = store

    def execute(self, query: str, output_name: str):
        for df_name, df in self._dfs.items():
            exec(f"{df_name} = df")
        self._dfs[output_name] = eval(query)
        for df_name in self._dfs.keys():
            if df_name != output_name:
                exec(f"del {df_name}")
        return self

    def get(self, table: str) -> DataFrame:
        return self._dfs[table]


class RExecuter():
    """
    R executer, based on rpy2:
    https://rpy2.readthedocs.io/en/latest/
    main: https://rpy2.readthedocs.io/en/latest/robjects_rinstance.html
    """
    language = "R"

    def __init__(self, store: Dict[str, DataFrame]):
        pandas2ri.activate()  # make the DataFrame converted to R on the fly
        self._renv = robjects
        self._dfs = store
        for df_name, df in store.items():
            # df_name is a str, df is a DataFrame
            self._renv.globalenv[df_name] = df

    def execute(self, query: str, output_name: str):
        self._renv.r(query)  # query is R code
        self._dfs[output_name] = self._renv.globalenv[output_name]
        return self

    def get(self,  table: str) -> DataFrame:
        return self._dfs[table]
