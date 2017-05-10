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
from graphscale.examples.todo.todo_pents import (
    create_todo_user,
    TodoUserInput,
    TodoUser,
    get_todo_config,
)

from graphscale.examples.todo.todo_graphql import create_todo_schema

from test.test_utils import MagnusConn

from graphql.execution.executors.asyncio import AsyncioExecutor

from flask_graphql import GraphQLView


def create_pent_context():
    edges = [KvetchDbEdgeDefinition(
        edge_name='user_to_todo_edge',
        edge_id=9283,
        from_id_attr='user_id')]

    pool = KvetchDbSingleConnectionPool(MagnusConn.get_unittest_conn())
    shard = KvetchDbShard(pool=pool)
    pent_context = PentContext(
        config=get_todo_config(),
        kvetch=Kvetch(shards=[shard], edges=edges, indexes=[]),
    )
    return pent_context

app = Flask(__name__)

outer_loop = asyncio.new_event_loop()
executor = AsyncioExecutor(loop=outer_loop)
app.add_url_rule('/graphql', 
    view_func=GraphQLView.as_view(
        'graphql', 
        schema=create_todo_schema(), 
        graphiql=True, 
        executor=executor,
        context=create_pent_context(),
        request=create_pent_context(),
    )
)

async def gen_main():
    pent_context = create_pent_context()
    user = await create_todo_user(pent_context, TodoUserInput(name='Joe'))
    return user

@app.route("/")
def hello():
    print(create_todo_schema())
    user_id = execute_gen(gen_main()).obj_id()
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
    Passed param check
    Another changed Hello World from Flask using Python 3.5
    """ + str(user_id)

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=8080)
