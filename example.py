import sys
import json
from main import ToucanConnectorsExecuter, SQLAlchemyExecuter, PandasExecuter


# ToucanTocoConnector usage:
if __name__ == '__main__':
    with open(sys.argv[1], "r") as f:
        configuration = json.load(f)

    store = ToucanConnectorsExecuter(configuration).get_dfs()
# --> get a "strore" (a.k.a a dictionary of DataFrame)


# SQLAlchemyExecuter usage:
    print(
        SQLAlchemyExecuter(store)
        .execute("SELECT * FROM trello_kanban_closed", "output_df")
        .get("output_df")
    )
# --> execute sql query on a store


# PandasExecuter usage:
    print(
        PandasExecuter(store)
        .execute("my_df.replace('CONFLICTING', \"lala\")")
        .get("output")
    )
# --> execute a pandas.DataFrame eval code on a store
