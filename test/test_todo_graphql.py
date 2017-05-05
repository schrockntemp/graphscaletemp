import asyncio

from graphql import (graphql)
from examples.todo.todo_graphql import create_todo_schema

from examples.todo.todo_pents import (
    get_todo_config,
    TodoUser,
    create_todo_user,
    TodoUserInput,
)

def create_fake_schema():
    return GraphQLSchema(
        query=GraphQLObjectType(
            name='Query',
            fields={
                'hello': GraphQLField(
                    type=GraphQLString,
                    resolver=lambda *_: 'world',
                ),
                'return_arg': GraphQLField(
                    type=GraphQLString,
                    args={
                        'value': GraphQLArgument(type=GraphQLString),
                    },
                    resolver=lambda _s, args, *_: args['value']
                ),
            },
        ),
    )

from graphql import (
    graphql,
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    GraphQLString,
    GraphQLArgument,
)

from graphscale.kvetch import Kvetch

from graphscale.kvetch.kvetch_memshard import (
    KvetchMemShard,
    KvetchMemIndexDefinition,
    KvetchMemEdgeDefinition,
)

from graphscale.pent.pent import (
    PentConfig,
    PentContext,
    Pent
)

from graphscale.utils import execute_coro

from graphql.execution.executors.asyncio import AsyncioExecutor

class TestContext:
    pass

def execute_todo_query(query, context_value=None):
    loop = asyncio.new_event_loop()
    result = graphql(
        create_todo_schema(),
        query,
        executor=AsyncioExecutor(loop=loop),
        context_value=context_value
    )
    if result.errors:
        raise Exception(str(result.errors))
    return result

def test_hello():
    query = '{ hello }'
    result = graphql(create_fake_schema(), query)
    assert result.data['hello'] == 'world'

def test_arg():
    query = '{ return_arg(value: "hello") }'
    result = graphql(create_fake_schema(), query)
    assert result.data['return_arg'] == 'hello'

def test_get_user():
    pent_context = mem_context()
    user_input = TodoUserInput(name='John Doe')
    user = execute_coro(create_todo_user(pent_context, user_input))
    assert user.name() == 'John Doe'
    new_id = user.id_()
    query = '{ user(id: "%s") { name } }' % new_id
    result = execute_todo_query(query, pent_context)
    assert result.data['user']['name'] == 'John Doe'

def mem_context():
    edges = [KvetchMemEdgeDefinition(
        edge_name='user_to_todo_edge',
        edge_id=9283,
        from_id_attr='user_id',
    )]
    shard = KvetchMemShard()
    kvetch = Kvetch(shards=[shard], edges=edges, indexes=[])
    pent_context = PentContext(
        kvetch=kvetch,
        config=get_todo_config()
    )
    return pent_context

