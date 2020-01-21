import sys
import json
from main import (
    ToucanConnectorsExecuter, SQLAlchemyExecuter,
    PandasExecuter, RExecuter, MongoExecuter
)


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
        .execute("my_df.replace('CONFLICTING', \"lala\")", "output")
        .get("output")
    )
# --> execute a pandas.DataFrame eval code on a store


# RExecuter usage:
    print(
        RExecuter(store)
        .execute('df$quantity <- df$A +1; output = df', 'output')
        .get("output")
    )
# --> execute a R code on the store


# MongoExecuter usage:
    print(
        MongoExecuter(store)
        .execute('[{"$addFields": { "new_col": 1} }]', 'output', 'input')
        .get('output')
    )
# --> execute a mongo pipeline the store
