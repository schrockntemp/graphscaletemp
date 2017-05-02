from uuid import uuid4
from examples.todo.todo_pents import (
    TodoUser,
    TodoUserInput,
    TodoItem,
    TodoItemInput,
    create_todo_user,
    create_todo_item,
    PentContext,
    get_todo_type_id_class_map,
    PentConfig,
    Pent,
)

# import pymysql

from graphscale.kvetch.kvetch import Kvetch
from graphscale.kvetch.kvetch_dbshard import (
    KvetchDbShard,
    KvetchDbSingleConnectionPool,
    KvetchDbEdgeDefinition,
    # KvetchDbIndexDefinition,
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

#W0621 display redefine variable for test fixture
#pylint: disable=W0621,C0103,W0401,W0614

# next task make this work with shard

import pytest

from .test_utils import *

def init_clear_kvetch_context(context):
    drop_objects_table(context.conn())
    drop_index_table(context.conn(), 'todo_items')
    create_kvetch_objects_table(context.conn())
    create_kvetch_index_table(context.conn(), 'todo_items')

@pytest.fixture
def test_cxt():
    return mem_context()
    # return db_context()

def db_context():
    KvetchMemIndexDefinition(indexed_attr='user_id', index_name='todo_item_user_index')
    edges = KvetchDbEdgeDefinition(
        edge_name='todo_to_user_edge',
        edge_id=9283,
        from_id_attr='user_id',
    )
    shards = [KvetchDbShard(
        pool=KvetchDbSingleConnectionPool(MagnusConn.get_unittest_conn()),
    )]
    drop_shard_db_tables(shards[0], {})
    init_shard_db_tables(shards[0], {})
    return PentContext(
        kvetch=Kvetch(shards=shards, edges=edges, indexes=[]),
        config=PentConfig(get_todo_type_id_class_map())
    )


def mem_context():
    edges = [KvetchMemEdgeDefinition(
        edge_name='todo_to_user_edge',
        edge_id=9283,
        from_id_attr='user_id',
    )]
    shards = [KvetchMemShard()]
    return PentContext(
        kvetch=Kvetch(shards=shards, edges=edges, indexes=[]),
        config=PentConfig(get_todo_type_id_class_map())
    )


def test_todo(test_cxt):
    id_ = uuid4()
    user = TodoUser(test_cxt, id_, {})
    assert id_ == user.id_()


@pytest.mark.asyncio
async def test_create_todo_user(test_cxt):
    new_user = await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))
    assert new_user.name() == 'Test Name'


@pytest.mark.asyncio
async def test_gen_user(test_cxt):
    new_user = await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))
    genned_user = await TodoUser.gen(test_cxt, new_user.id_())
    assert genned_user.name() == 'Test Name'


@pytest.mark.asyncio
async def test_gen_pent(test_cxt):
    new_user = await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))
    genned_pent = await Pent.gen(test_cxt, new_user.id_())
    assert isinstance(genned_pent, TodoUser)
    assert genned_pent.name() == 'Test Name'


@pytest.mark.asyncio
async def test_gen_pents(test_cxt):
    new_user_id = (await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))).id_()
    todo_input = TodoItemInput(user_id=new_user_id, text='some text')
    new_todo_id = (await create_todo_item(test_cxt, todo_input)).id_()
    pent_dict = await Pent.gen_list(test_cxt, [new_user_id, new_todo_id])
    assert len(pent_dict) == 2
    assert isinstance(pent_dict[new_user_id], TodoUser)
    assert isinstance(pent_dict[new_todo_id], TodoItem)


@pytest.mark.asyncio
async def test_gen_item(test_cxt):
    new_user = await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))
    todo_input = TodoItemInput(user_id=new_user.id_(), text='some text')
    new_todo_item = await create_todo_item(test_cxt, todo_input)
    assert new_todo_item.text() == 'some text'


@pytest.mark.asyncio
async def test_gen_item_then_user(test_cxt):
    new_user = await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))
    todo_input = TodoItemInput(user_id=new_user.id_(), text='some text')
    new_todo_item = await create_todo_item(test_cxt, todo_input)
    genned_user = await new_todo_item.gen_user()
    assert genned_user.id_() == new_user.id_()


@pytest.mark.asyncio
async def test_gen_item_list_one_member(test_cxt):
    new_user_id = (await create_todo_user(test_cxt, TodoUserInput(name='Test Name'))).id_()
    todo_input = TodoItemInput(user_id=new_user_id, text='some text')
    new_todo_id = (await create_todo_item(test_cxt, todo_input)).id_()
    new_user = await TodoUser.gen(test_cxt, new_user_id)
    new_todos = await new_user.gen_todo_items()
    assert len(new_todos) == 1
    assert new_todos[0].id_() == new_todo_id
    assert new_todos[0].text() == 'some text'

# @pytest.mark.asyncio
# async def test_gen_item_list_three_members(test_context):
#     new_user_id = (await create_todo_user(test_context, TodoUserInput(name='Test Name'))).id_()
#     todo1_id = (await create_todo_item(
#         test_context, TodoItemInput(user_id=new_user_id, text='text1'))).id_()
#     todo2_id = (await create_todo_item(
#         test_context, TodoItemInput(user_id=new_user_id, text='text2'))).id_()
#     todo3_id = (await create_todo_item(
#         test_context, TodoItemInput(user_id=new_user_id, text='text3'))).id_()
#     new_user = await TodoUser.gen(test_context, new_user_id)
#     new_todos = await new_user.gen_todo_items()
#     assert len(new_todos) == 3
#     assert new_todos[0].id_() == todo1_id
#     assert new_todos[0].text() == 'text1'
#     assert new_todos[1].id_() == todo2_id
#     assert new_todos[1].text() == 'text2'
#     assert new_todos[2].id_() == todo3_id
#     assert new_todos[2].text() == 'text3'
