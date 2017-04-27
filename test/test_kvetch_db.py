from uuid import UUID, uuid4

import pymysql
import pymysql.cursors

#W0621 display redefine variable for test fixture
#pylint: disable=W0621,C0103,W0401,W0614

import pytest

from graphscale.kvetch.kvetch_db import *
from graphscale.kvetch.kvetch_dbshard import (
    KvetchDbShard,
    KvetchDbShardIndex,
    KvetchDbSingleConnectionPool,
    sync_kv_insert_object,
    sync_kv_get_object,
    sync_kv_get_objects,
    sync_kv_index_get_all,
)

from graphscale.kvetch.kvetch_memshard import (
    KvetchMemShard,
    KvetchMemShardIndex,
)

from graphscale.kvetch.kvetch_dbschema import (
    create_kvetch_objects_table,
    create_kvetch_index_table,
    drop_shard_db_tables,
    init_shard_db_tables,
)

# def init_clear_kvetch_context(conn):
#     drop_objects_table(conn)
#     drop_index_table(conn, 'test')
#     create_kvetch_objects_table(conn)
#     create_kvetch_index_table(conn, 'test')

# magnus_conn = pymysql.connect(
#     host='localhost',
#     user='magnus',
#     password='magnus',
#     db='unittest_mysql_db',
#     charset='utf8mb4',
#     cursorclass=pymysql.cursors.DictCursor)

@pytest.fixture
def test_shard_single_index():
    related_index = KvetchMemShardIndex(
        indexed_attr='related_id',
        index_name='related_id_index',
    )
    # shard = KvetchDbShard(pool=KvetchDbSingleConnectionPool(magnus_conn), indexes=[related_index])
    shard = KvetchMemShard(indexes=[related_index])
    # drop_shard_db_tables(shard)
    # init_shard_db_tables(shard)
    yield shard

@pytest.fixture
def test_shard_double_index():
    related_index = KvetchDbShardIndex(
        indexed_attr='related_id',
        indexed_sql_type='BINARY(16)',
        index_name='related_id_index',
    )
    num_index = KvetchDbShardIndex(
        indexed_attr='num',
        indexed_sql_type='INT',
        index_name='num_index'
    )
    shard = KvetchDbShard(
        pool=KvetchDbSingleConnectionPool(magnus_conn),
        indexes=[related_index, num_index]
    )
    drop_shard_db_tables(shard)
    init_shard_db_tables(shard)
    yield shard

def test_data_to_body():
    body = data_to_body({'name' : 'Joe'})
    assert body is not None

def test_object_insert(test_shard_single_index):
    data = {'num' : 2}
    new_id = sync_kv_insert_object(test_shard_single_index, 1000, data)
    assert isinstance(new_id, UUID)

def test_object_insert_get(test_shard_single_index):
    data = {'num' :3}
    new_id = sync_kv_insert_object(test_shard_single_index, 1000, data)
    new_data = sync_kv_get_object(test_shard_single_index, new_id)
    assert new_data['id'] == new_id
    assert new_data['num'] == 3
    assert new_data['__type_id'] == 1000

def test_objects_insert_get(test_shard_single_index):
    data_one = {'num' : 4}
    data_two = {'num' : 5}
    id_one = sync_kv_insert_object(test_shard_single_index, 1000, data_one)
    id_two = sync_kv_insert_object(test_shard_single_index, 1000, data_two)
    obj_dict = sync_kv_get_objects(test_shard_single_index, [id_one, id_two])
    assert len(obj_dict) == 2
    assert obj_dict[id_one]['id'] == id_one
    assert obj_dict[id_one]['num'] == 4
    assert obj_dict[id_two]['id'] == id_two
    assert obj_dict[id_two]['num'] == 5

def test_get_zero_objects(test_shard_single_index):
    with pytest.raises(ValueError):
        sync_kv_get_objects(test_shard_single_index, [])

def test_object_insert_id(test_shard_single_index):
    with pytest.raises(ValueError):
        sync_kv_insert_object(test_shard_single_index, 1000, None)
    with pytest.raises(ValueError):
        sync_kv_insert_object(test_shard_single_index, 1000, [])
    with pytest.raises(ValueError):
        sync_kv_insert_object(test_shard_single_index, 1000, {'id' : 234})
    with pytest.raises(ValueError):
        sync_kv_insert_object(test_shard_single_index, 1000, {'__type_id' : 234})
    with pytest.raises(ValueError):
        sync_kv_insert_object(test_shard_single_index, None, {'name' : 'Joe'})

def test_null_shard():
    with pytest.raises(ValueError):
        sync_kv_insert_object(None, 1000, {'num' : 234})
    with pytest.raises(ValueError):
        sync_kv_get_object(None, uuid4())
    with pytest.raises(ValueError):
        sync_kv_get_objects(None, [uuid4()])

def test_bad_args(test_shard_single_index):
    with pytest.raises(ValueError):
        sync_kv_get_object(test_shard_single_index, None)

def test_insert_index_entry(test_shard_single_index):
    data_one = {'num' : 4}
    id_one = sync_kv_insert_object(test_shard_single_index, 1000, data_one)
    data_two = {'num' : 5, 'related_id' : id_one}
    sync_kv_insert_object(test_shard_single_index, 1000, data_two)

def test_single_index(test_shard_single_index):
    data_one = {'num' : 4}
    id_one = sync_kv_insert_object(test_shard_single_index, 1000, data_one)
    data_two = {'num' : 5, 'related_id' : id_one}
    id_two = sync_kv_insert_object(test_shard_single_index, 1000, data_two)
    data_three = {'num' : 6, 'related_id' : None}
    sync_kv_insert_object(test_shard_single_index, 1000, data_three)
    related_index = test_shard_single_index.index_by_name('related_id_index')
    target_objs = sync_kv_index_get_all(test_shard_single_index, related_index, id_one)
    assert len(target_objs) == 1
    target_obj = target_objs[id_two]
    assert target_obj['id'] == id_two
    assert target_obj['num'] == 5

# def sort_ids(ids):
#     return sorted(ids, key=lambda id_: id_.bytes)

# def test_double_index(test_shard_double_index):
#     data_one = {'num' : 4, 'related_id' : None, 'text' : 'first insert'}
#     id_one = sync_kv_insert_object(test_shard_double_index, 1000, data_one)
#     data_two = {'num' : 4, 'related_id' : id_one, 'text' : 'second insert'}
#     # this will insert into both the related_id_index and the num_index
#     id_two = sync_kv_insert_object(test_shard_double_index, 1000, data_two)
#     data_three = {'num' : 5, 'related_id' : id_one, 'text' : 'third insert'}
#     id_three = sync_kv_insert_object(test_shard_double_index, 1000, data_three)

#     num_index = test_shard_double_index.index_by_name('num_index')
#     num4_objs = sync_kv_index_get_all(test_shard_double_index, num_index, 4)
#     assert num4_objs[id_one]['text'] == 'first insert'
#     assert num4_objs[id_two]['text'] == 'second insert'

#     related_id_index = test_shard_double_index.index_by_name('related_id_index')
#     related_objs = sync_kv_index_get_all(test_shard_double_index, related_id_index, id_one)
#     assert len(related_objs) == 2
#     assert related_objs[id_two]['text'] == 'second insert'
#     assert related_objs[id_three]['text'] == 'third insert'
