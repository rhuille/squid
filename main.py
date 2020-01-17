
import importlib
import json
import sys

from toucan_connectors import (
    CONNECTORS_CATALOGUE, ToucanConnector, ToucanDataSource)
from sqlalchemy import create_engine


def open_configuration():
    with open(sys.argv[1], "r") as f:
        configuration = json.load(f)
    return configuration


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


def load_data_in_sql(configuration: list, engine):
    """
    From configuration, load all data source in a table in sql engine
    """
    for data_source_conf in configuration["DATA_SOURCES"]:
        connector_conf = get_connector_from_data_source(
            data_source_conf, configuration)
        connector, data_source = connector_and_data_source_from_conf(
            connector_conf, data_source_conf)
        df = connector.get_df(data_source)
        df[df.columns] = df[df.columns].astype(str)
        df.to_sql(data_source_conf['domain'], con=engine)
        print(f"{data_source_conf['domain']} loaded")


if __name__ == '__main__':
    engine = create_engine('sqlite://', echo=False)
    configuration = open_configuration()
    print('configuration loaded')

    load_data_in_sql(configuration, engine)

    sql_query = input("sql query:")
    print(engine.execute(sql_query).fetchall())
