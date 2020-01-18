
import importlib
from typing import Dict

from pandas import DataFrame
from toucan_connectors import (
    CONNECTORS_CATALOGUE, ToucanConnector, ToucanDataSource)
from sqlalchemy import create_engine


def connector_and_data_source_from_conf(
    connector_conf: dict,
    data_source_conf: dict
) -> (ToucanConnector, ToucanDataSource):
    # Get connector and data_source pydantic models:
    path_to_module, connector_class_name = (
        CONNECTORS_CATALOGUE[connector_conf['type']].rsplit('.', 1)
    )
    connector_module = importlib.import_module(
        f".{path_to_module}",
        package="toucan_connectors"
    )
    connector_model = getattr(connector_module, connector_class_name)
    data_source_model = connector_model.data_source_model

    # Apply configuration: (pydantic may raise a Validation error)
    connector = connector_model(**connector_conf)
    data_source = data_source_model(**data_source_conf)

    return connector, data_source


def get_connector_from_data_source(
    data_source_conf: dict,
    configuration: dict
) -> dict:
    """
    From a data source configuration return its corresponding connector
    configuration

    Example:
    --------
    data_source_conf = {'name': 'cerbere', 'domain': 'my_domain', ...}
    configuration = {
        'CONNECTORS':[{'name': 'cerbere', 'type': 'HttpAPI', ...}, ...],
        'DATA_SOURCES':[...]
    }
    will return: {'name': 'cerbere', 'type': 'HttpAPI', ...}
    """
    for connector_conf in configuration['CONNECTORS']:
        if connector_conf['name'] == data_source_conf['name']:
            return connector_conf


def toucan_connector_executer(configuration: dict) -> Dict[str, DataFrame]:
    """
    Input: a dict of configuration of `ToucanConnectors` and `ToucanDataSource`
    Output: a dictionary which values are pandas DataFrame
    """
    # TODO: `configuration` validation
    store = {}
    for data_source_conf in configuration["DATA_SOURCES"]:
        connector_conf = get_connector_from_data_source(
            data_source_conf, configuration)
        connector, data_source = connector_and_data_source_from_conf(
            connector_conf, data_source_conf)
        store[data_source_conf['domain']] = connector.get_df(data_source)
    return store


def sql_query_executer(store: Dict[str, DataFrame], query):
    engine = create_engine('sqlite:///:memory:', echo=False)
    for df_name, df in store.items():
        # df_name is a str, df is a DataFrame
        df[df.columns] = df[df.columns].astype(str)  # TODO: remove this line
        df.to_sql(df_name, con=engine)
    return engine.execute(query).fetchall()
