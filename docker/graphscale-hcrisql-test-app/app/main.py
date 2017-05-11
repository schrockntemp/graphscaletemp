#pylint: disable=C0103

import asyncio

from flask import Flask

import pymysql

from graphql.execution.executors.asyncio import AsyncioExecutor

from graphscale.kvetch.kvetch import Kvetch

from graphscale.kvetch.kvetch_dbshard import (
    KvetchDbShard,
    KvetchDbSingleConnectionPool,
    # KvetchDbEdgeDefinition,
    KvetchDbIndexDefinition,
)

from graphscale.kvetch.kvetch_dbschema import (
    init_shard_db_tables
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
# from graphscale.utils import param_check, execute_gen

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

    index = KvetchDbIndexDefinition(
        indexed_attr='provider',
        indexed_sql_type='CHAR(255)',
        index_name='provider_index',
    )

    # drop_shard_db_tables(shards[0], {})
    init_shard_db_tables(shards[0], {
        'provider_index': index
    })
    return Kvetch(shards=shards, edges=[], indexes=[index])

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
app.add_url_rule(
    '/graphql',
    view_func=GraphQLView.as_view(
        'graphql',
        schema=create_hcris_schema(),
        graphiql=True,
        executor=executor,
        context=create_pent_context(),
    ),
)

@app.route("/")
def hello():
    return """
    Goto http://localhost:8080/graphql
    """

if __name__ == "__main__":
    app.run(host='0.0.0.0', debug=True, port=8080)
