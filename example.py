import sys
import json
from main import ToucanConnectorsExecuter, sql_query_executer


if __name__ == '__main__':
    with open(sys.argv[1], "r") as f:
        configuration = json.load(f)

    store = ToucanConnectorsExecuter(configuration).get_dfs()
    sql_query = """
    SELECT * FROM trello_kanban_closed
    """
    result = sql_query_executer(store, sql_query)
    print(result)
