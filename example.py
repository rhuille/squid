import sys
import json
from main import toucan_connector_executer, sql_query_executer


if __name__ == '__main__':
    with open(sys.argv[1], "r") as f:
        configuration = json.load(f)

    store = toucan_connector_executer(configuration)
    sql_query = """
    SELECT * FROM trello_kanban_closed
    """
    result = sql_query_executer(store, sql_query)
    print(result)
