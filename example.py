import sys
import json
from main import ToucanConnectorsExecuter, SQLAlchemyExecuter


if __name__ == '__main__':
    with open(sys.argv[1], "r") as f:
        configuration = json.load(f)

    store = ToucanConnectorsExecuter(configuration).get_dfs()
    sql_query = """
    SELECT * FROM trello_kanban_closed
    """
    db = SQLAlchemyExecuter(store)
    result = db.execute(sql_query, "output").get("output")
    print(result)
