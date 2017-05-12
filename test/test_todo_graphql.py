from uuid import UUID

from graphql import (
    graphql,
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    GraphQLString,
    GraphQLArgument,
)

from graphscale.examples.todo.todo_graphql import create_todo_schema
from graphscale.examples.todo.todo_pents import (
    get_todo_config,
    create_todo_user,
    create_todo_item,
    TodoUserInput,
    TodoItemInput,
)
from graphscale.kvetch import Kvetch
from graphscale.kvetch.kvetch_memshard import (
    KvetchMemShard,
    KvetchMemEdgeDefinition,
)
from graphscale.pent.pent import PentContext
from graphscale.utils import execute_gen
from test.test_utils import execute_test_graphql

def execute_todo_query(query, pent_context):
    return execute_test_graphql(query, pent_context, create_todo_schema())

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

def test_hello():
    query = '{ hello }'
    result = graphql(create_fake_schema(), query)
    assert result.data['hello'] == 'world'

def test_arg():
    query = '{ return_arg(value: "hello") }'
    result = graphql(create_fake_schema(), query)
    assert result.data['return_arg'] == 'hello'

def get_user_name_via_graphql(pent_context, obj_id):
    query = '{ user(id: "%s") { name } }' % obj_id
    return execute_todo_query(query, pent_context)

def create_user_via_graphql(pent_context, name):
    mutation_query = """
    mutation {
        createUser(input: {
            name: "%s"
        }) {
            id
            name
        }
    }
""" % name
    return execute_todo_query(mutation_query, pent_context)

def test_get_user():
    pent_context = mem_context()
    user_input = TodoUserInput(name='John Doe')
    user = execute_gen(create_todo_user(pent_context, user_input))
    assert user.name() == 'John Doe'
    new_id = user.obj_id()
    result = get_user_name_via_graphql(pent_context, new_id)
    assert result.data['user']['name'] == 'John Doe'

def test_get_computed_field():
    pent_context = mem_context()
    user_input = TodoUserInput(name='John Doe')
    user = execute_gen(create_todo_user(pent_context, user_input))
    assert user.name() == 'John Doe'
    new_id = user.obj_id()
    query = '{ user (id: "%s") { capitalizedName } }' % new_id
    result = execute_todo_query(query, pent_context)
    assert result.data['user']['capitalizedName'] == 'JOHN DOE'

def test_create_user():
    pent_context = mem_context()
    mutation_result = create_user_via_graphql(pent_context, 'John Doe')
    assert mutation_result.data['createUser']['name'] == 'John Doe'
    new_id = UUID(mutation_result.data['createUser']['id'])
    query = '{ user (id: "%s") { name } }' % new_id
    result = execute_todo_query(query, pent_context)
    assert result.data['user']['name'] == 'John Doe'

def test_create_update_user():
    pent_context = mem_context()
    create_result = create_user_via_graphql(pent_context, 'John Doe')
    assert create_result.data['createUser']['name'] == 'John Doe'
    new_id = UUID(create_result.data['createUser']['id'])
    get_result = get_user_name_via_graphql(pent_context, new_id)
    assert get_result.data['user']['name'] == 'John Doe'

    update_query = """
    mutation { 
        updateUser(id: "%s", input: { name: "%s" }) {
            id
            name
        }
    }
""" % (new_id, 'Jane Doe')
    update_result = execute_todo_query(update_query, pent_context)
    assert update_result.data['updateUser']['name'] == 'Jane Doe'

def test_create_delete_user():
    pent_context = mem_context()
    create_result = create_user_via_graphql(pent_context, 'John Doe')
    assert create_result.data['createUser']['name'] == 'John Doe'
    new_id = UUID(create_result.data['createUser']['id'])

    delete_query = """
    mutation {
        deleteUser(id: "%s") {
            id
        }
    }""" % new_id

    execute_todo_query(delete_query, pent_context)
    get_result = get_user_name_via_graphql(pent_context, new_id)
    assert get_result.data['user'] is None

def test_get_item():
    pent_context = mem_context()
    user_input = TodoUserInput(name='John Doe')
    user = execute_gen(create_todo_user(pent_context, user_input))
    todo_input_one = TodoItemInput(user_id=user.obj_id(), text='something one')
    todo_one = execute_gen(create_todo_item(pent_context, todo_input_one))
    assert todo_one.text() == 'something one'

    todo_input_two = TodoItemInput(user_id=user.obj_id(), text='something two')
    todo_two = execute_gen(create_todo_item(pent_context, todo_input_two))
    assert todo_two.text() == 'something two'

    todo_user_out = execute_gen(todo_one.gen_user())
    assert todo_user_out.name() == 'John Doe'

    query = '{ todoItem(id: "%s") { text } }' % todo_one.obj_id()
    result = execute_todo_query(query, pent_context)
    assert result.data['todoItem']['text'] == 'something one'

    query = '{ user (id: "%s") { todoItems { text } } }' % user.obj_id()
    result = execute_todo_query(query, pent_context)

    assert len(result.data['user']['todoItems']) == 2
    assert result.data['user']['todoItems'][0]['text'] == 'something one'
    assert result.data['user']['todoItems'][1]['text'] == 'something two'

    first_one_todo_items = execute_gen(user.gen_todo_items(first=1))
    assert len(first_one_todo_items) == 1
    assert first_one_todo_items[0].text() == 'something one'

    query = '{ user (id: "%s") { todoItems(first: 1) { text } } }' % user.obj_id()
    result = execute_todo_query(query, pent_context)
    assert result.data['user']['todoItems'][0]['text'] == 'something one'


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

