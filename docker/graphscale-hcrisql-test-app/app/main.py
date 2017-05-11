import asyncio

from flask import Flask

from graphscale.utils import param_check, execute_gen

from graphscale.kvetch.kvetch import Kvetch

from graphql import graphql

from graphscale.kvetch.kvetch_dbshard import (
    KvetchDbShard,
    KvetchDbSingleConnectionPool,
    KvetchDbEdgeDefinition,
)

from graphscale.pent.pent import (
    PentContext
)

from graphscale.examples.hcris.hcris_pent import (
    get_hcris_config
)

from graphscale.examples.hcris.hcris_graphql import (
    create_hcris_schema
)

import pymysql

# from examples.todo.todo_pents import (
#     create_todo_user,
#     TodoUserInput,
#     TodoUser,
#     get_todo_config,
# )

# from examples.todo.todo_graphql import create_todo_schema

# from test.test_utils import MagnusConn

from graphql.execution.executors.asyncio import AsyncioExecutor

from flask_graphql import GraphQLView

def get_kvetch():
    hcrisql_conn = pymysql.connect(
        host='localhost',
        user='magnus',
        password='magnus',
        db='graphscale-hcrisql-db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

    shards = [KvetchDbShard(
        pool=KvetchDbSingleConnectionPool(hcrisql_conn),
    )]

    # drop_shard_db_tables(shards[0], {})
    # init_shard_db_tables(shards[0], {})
    return Kvetch(shards=shards, edges=[], indexes=[])

def create_pent_context():
    kvetch = get_kvetch()
    pent_context = PentContext(
        config=get_hcris_config(),
        kvetch=kvetch,
    )
    return pent_context

app = Flask(__name__)

outer_loop = asyncio.new_event_loop()
executor = AsyncioExecutor(loop=outer_loop)
app.add_url_rule('/graphql', 
    view_func=GraphQLView.as_view(
        'graphql', 
        schema=create_hcris_schema(), 
        graphiql=True, 
        executor=executor,
        context=create_pent_context(),
        request=create_pent_context(),
    )
)


@app.route("/")
def hello():
    print('DOES THIS WORK')
    # query = '{ user(id: "%s") { id, name } }' % user_id
    # pent_context = create_pent_context()
    # loop = asyncio.new_event_loop()
    # result = graphql(
    #     create_todo_schema(),
    #     query,
    #     executor=AsyncioExecutor(loop=loop),
    #     context_value=pent_context
    # )
    return """
    Goto http://localhost:8080/graphql
    """ 

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=8080)
