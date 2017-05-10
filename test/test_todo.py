#W0621 display redefine variable for test fixture
#pylint: disable=W0621,C0103,W0401,W0614
from uuid import uuid4

import pytest

from graphscale.examples.todo.todo_pents import (
    TodoItem,
    TodoItemInput,
    TodoUser,
    TodoUserInput,
    create_todo_item,
    create_todo_user,
    update_todo_user,
    get_todo_config
)

from graphscale.kvetch.kvetch import Kvetch
from graphscale.kvetch.kvetch_dbshard import (
    KvetchDbShard,
    KvetchDbSingleConnectionPool,
    KvetchDbEdgeDefinition,
)

from graphscale.kvetch.kvetch_memshard import (
    KvetchMemShard,
    KvetchMemIndexDefinition,
    KvetchMemEdgeDefinition,
)

from graphscale.kvetch.kvetch_dbschema import (
    init_shard_db_tables,
    drop_shard_db_tables,
)

from graphscale.pent.pent import (
    PentContext,
    Pent
)

from .test_utils import MagnusConn, db_mem_fixture

def db_context():
    edges = [KvetchDbEdgeDefinition(
        edge_name='user_to_todo_edge',
        edge_id=9283,
        from_id_attr='user_id',
    )]
    shards = [KvetchDbShard(
        pool=KvetchDbSingleConnectionPool(MagnusConn.get_unittest_conn()),
    )]
    drop_shard_db_tables(shards[0], {})
    init_shard_db_tables(shards[0], {})
    return PentContext(
        kvetch=Kvetch(shards=shards, edges=edges, indexes=[]),
        config=get_todo_config()
    )

def mem_context():
    edges = [KvetchMemEdgeDefinition(
        edge_name='user_to_todo_edge',
        edge_id=9283,
        from_id_attr='user_id',
    )]
    shards = [KvetchMemShard()]
    return PentContext(
        kvetch=Kvetch(shards=shards, edges=edges, indexes=[]),
        config=get_todo_config()
    )

@pytest.fixture(params=db_mem_fixture(mem=mem_context, db=db_context))
def test_cxt(request):
    return request.param()

def test_todo(test_cxt):
    obj_id = uuid4()
    user = TodoUser(test_cxt, obj_id, {})
    assert obj_id == user.obj_id()

@pytest.mark.asyncio
async def test_create_todo_user(test_cxt):
    new_user = await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))
    assert new_user.name() == 'Test Name'

@pytest.mark.asyncio
async def test_create_update_user(test_cxt):
    user_t_one = await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))
    new_id = user_t_one.obj_id()
    assert user_t_one.name() == 'Test Name'
    await update_todo_user(test_cxt, new_id, TodoUserInput(name='New Name'))
    user_t_two = await TodoUser.gen(test_cxt, new_id)
    assert user_t_two.name() == 'New Name'

@pytest.mark.asyncio
async def test_gen_user(test_cxt):
    new_user = await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))
    genned_user = await TodoUser.gen(test_cxt, new_user.obj_id())
    assert genned_user.name() == 'Test Name'


@pytest.mark.asyncio
async def test_gen_pent(test_cxt):
    new_user = await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))
    genned_pent = await Pent.gen(test_cxt, new_user.obj_id())
    assert isinstance(genned_pent, TodoUser)
    assert genned_pent.name() == 'Test Name'


@pytest.mark.asyncio
async def test_gen_pents(test_cxt):
    new_user_id = (await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))).obj_id()
    todo_input = TodoItemInput(user_id=new_user_id, text='some text')
    new_todo_id = (await create_todo_item(test_cxt, todo_input)).obj_id()
    pent_dict = await Pent.gen_list(test_cxt, [new_user_id, new_todo_id])
    assert len(pent_dict) == 2
    assert isinstance(pent_dict[new_user_id], TodoUser)
    assert isinstance(pent_dict[new_todo_id], TodoItem)


@pytest.mark.asyncio
async def test_gen_item(test_cxt):
    new_user = await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))
    todo_input = TodoItemInput(user_id=new_user.obj_id(), text='some text')
    new_todo_item = await create_todo_item(test_cxt, todo_input)
    assert new_todo_item.text() == 'some text'


@pytest.mark.asyncio
async def test_gen_item_then_user(test_cxt):
    new_user = await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))
    todo_input = TodoItemInput(user_id=new_user.obj_id(), text='some text')
    new_todo_item = await create_todo_item(test_cxt, todo_input)
    genned_user = await new_todo_item.gen_user()
    assert genned_user.obj_id() == new_user.obj_id()


@pytest.mark.asyncio
async def test_gen_item_list_one_member(test_cxt):
    new_user_id = (await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))).obj_id()
    todo_input = TodoItemInput(user_id=new_user_id, text='some text')
    new_todo_id = (await create_todo_item(test_cxt, todo_input)).obj_id()
    new_user = await TodoUser.gen(test_cxt, new_user_id)
    new_todos = await new_user.gen_todo_items()
    assert len(new_todos) == 1
    assert new_todos[0].obj_id() == new_todo_id
    assert new_todos[0].text() == 'some text'

@pytest.mark.asyncio
async def test_gen_item_list_three_members(test_cxt):
    new_user_id = (await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))).obj_id()
    todo1_id = (await create_todo_item(
        test_cxt, TodoItemInput(user_id=new_user_id, text='text1'))).obj_id()
    todo2_id = (await create_todo_item(
        test_cxt, TodoItemInput(user_id=new_user_id, text='text2'))).obj_id()
    todo3_id = (await create_todo_item(
        test_cxt, TodoItemInput(user_id=new_user_id, text='text3'))).obj_id()
    new_user = await TodoUser.gen(test_cxt, new_user_id)
    new_todos = await new_user.gen_todo_items()
    assert len(new_todos) == 3
    assert new_todos[0].obj_id() == todo1_id
    assert new_todos[0].text() == 'text1'
    assert new_todos[1].obj_id() == todo2_id
    assert new_todos[1].text() == 'text2'
    assert new_todos[2].obj_id() == todo3_id
    assert new_todos[2].text() == 'text3'

def test_data_valid_check():
    assert TodoUser.is_input_data_valid({'id': uuid4(), 'name': 'something'})
    assert not TodoUser.is_input_data_valid(None)
    assert not TodoUser.is_input_data_valid('str')
    assert not TodoUser.is_input_data_valid({'id': None, 'name': 'something'})
    assert not TodoUser.is_input_data_valid({'id': 2343, 'name': 'something'})
    assert not TodoUser.is_input_data_valid({'name': 'something'})
    assert not TodoUser.is_input_data_valid({'id': 2343})
    assert not TodoUser.is_input_data_valid({'id': 2343, 'name': None})
    assert not TodoUser.is_input_data_valid({'id': 2343, 'name': 39483948})
