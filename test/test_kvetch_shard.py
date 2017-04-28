from uuid import UUID, uuid4

import pymysql
import pymysql.cursors

#W0621 display redefine variable for test fixture
#pylint: disable=W0621,C0103,W0401,W0614

import pytest

from graphscale.kvetch.kvetch_dbshard import (
    KvetchDbShard,
    KvetchDbIndex,
    KvetchDbSingleConnectionPool,
    sync_kv_insert_object,
    sync_kv_get_object,
    sync_kv_get_objects,
    sync_kv_insert_index_entry,
    sync_kv_get_index_ids,
)

from graphscale.kvetch.kvetch_memshard import (
    KvetchMemShard,
    KvetchMemIndex,
)

from graphscale.kvetch.kvetch_dbschema import (
    drop_shard_db_tables,
    init_shard_db_tables,
)

from .test_utils import MagnusConn

def mem_single_index_shard():
    related_index = KvetchMemIndex(
        indexed_attr='related_id',
        index_name='related_id_index',
    )
    indexes = {
        'related_id_index': related_index,
    }
    return (KvetchMemShard(), indexes)


def db_single_index_shard():
    related_index = KvetchDbIndex(
        indexed_attr='related_id',
        indexed_sql_type='BINARY(16)',
        index_name='related_id_index',
    )
    shard = KvetchDbShard(
        pool=KvetchDbSingleConnectionPool(MagnusConn.get_conn()),
    )
    indexes = {
        'related_id_index': related_index,
    }
    drop_shard_db_tables(shard, indexes)
    init_shard_db_tables(shard, indexes)
    return (shard, indexes)

@pytest.fixture
def only_shard():
    shard, _ = test_shard_single_index()
    return shard

@pytest.fixture
def test_shard_single_index():
    return mem_single_index_shard()
    # return db_single_index_shard()

@pytest.fixture
def test_shard_double_index():
    return mem_double_index_shard()
    # return db_double_index_shard()

def mem_double_index_shard():
    related_index = KvetchMemIndex(
        indexed_attr='related_id',
        index_name='related_id_index',
    )
    num_index = KvetchMemIndex(
        indexed_attr='num',
        index_name='num_index'
    )
    indexes = {'related_id_index': related_index, 'num_index': num_index}
    return (KvetchMemShard(), indexes)


def db_double_index_shard():
    related_index = KvetchDbIndex(
        indexed_attr='related_id',
        indexed_sql_type='BINARY(16)',
        index_name='related_id_index',
    )
    num_index = KvetchDbIndex(
        indexed_attr='num',
        indexed_sql_type='INT',
        index_name='num_index'
    )
    shard = KvetchDbShard(
        pool=KvetchDbSingleConnectionPool(MagnusConn.get_conn()),
    )
    indexes = {'related_id_index': related_index, 'num_index': num_index}
    drop_shard_db_tables(shard, indexes)
    init_shard_db_tables(shard, indexes)
    return shard, indexes



def insert_test_obj(shard, data):
    new_id = uuid4()
    sync_kv_insert_object(shard, new_id, 1000, data)
    return new_id


def test_object_insert(only_shard):
    shard = only_shard
    data = {'num': 2}
    new_id = insert_test_obj(shard, data)
    assert isinstance(new_id, UUID)


def test_object_insert_get(only_shard):
    shard = only_shard
    data = {'num': 3}
    new_id = insert_test_obj(shard, data)
    new_data = sync_kv_get_object(shard, new_id)
    assert new_data['id'] == new_id
    assert new_data['num'] == 3
    assert new_data['__type_id'] == 1000


def test_objects_insert_get(only_shard):
    shard = only_shard
    data_one = {'num': 4}
    data_two = {'num': 5}
    id_one = insert_test_obj(shard, data_one)
    # sync_kv_insert_object(test_shard_single_index, id_one, 1000, data_one)
    id_two = insert_test_obj(shard, data_two)
    # sync_kv_insert_object(test_shard_single_index, id_two, 1000, data_two)
    obj_dict = sync_kv_get_objects(shard, [id_one, id_two])
    assert len(obj_dict) == 2
    assert obj_dict[id_one]['id'] == id_one
    assert obj_dict[id_one]['num'] == 4
    assert obj_dict[id_two]['id'] == id_two
    assert obj_dict[id_two]['num'] == 5


def test_get_zero_objects(only_shard):
    with pytest.raises(ValueError):
        sync_kv_get_objects(only_shard, [])


def test_object_insert_id(only_shard):
    new_id = uuid4()
    with pytest.raises(ValueError):
        sync_kv_insert_object(only_shard, new_id, 1000, None)
    with pytest.raises(ValueError):
        sync_kv_insert_object(only_shard, new_id, 1000, [])
    with pytest.raises(ValueError):
        sync_kv_insert_object(only_shard,
                              new_id, 1000, {'id': 234})
    with pytest.raises(ValueError):
        sync_kv_insert_object(only_shard,
                              new_id, 1000, {'__type_id': 234})
    with pytest.raises(ValueError):
        sync_kv_insert_object(only_shard,
                              new_id, None, {'name': 'Joe'})
    with pytest.raises(ValueError):
        sync_kv_insert_object(only_shard, None, 1000, {})
    with pytest.raises(ValueError):
        sync_kv_insert_object(only_shard, 101, 1000, {})


def test_null_shard():
    with pytest.raises(ValueError):
        sync_kv_insert_object(None, uuid4(), 1000, {'num': 234})
    with pytest.raises(ValueError):
        sync_kv_get_object(None, uuid4())
    with pytest.raises(ValueError):
        sync_kv_get_objects(None, [uuid4()])


def test_bad_args(only_shard):
    with pytest.raises(ValueError):
        sync_kv_get_object(only_shard, None)


def test_id_index(test_shard_single_index):
    shard, indexes = test_shard_single_index
    data_one = {'num': 4}
    id_one = insert_test_obj(shard, data_one)
    data_two = {'num': 5, 'related_id': id_one}
    id_two = insert_test_obj(shard, data_two)
    data_three = {'num': 6, 'related_id': id_one}
    id_three = insert_test_obj(shard, data_three)

    index_name = 'related_id_index'
    related_index = indexes[index_name]
    assert related_index is not None
    sync_kv_insert_index_entry(shard, related_index, id_one, id_two)
    sync_kv_insert_index_entry(shard, related_index, id_one, id_three)
    ids = sync_kv_get_index_ids(shard, related_index, id_one)
    assert len(ids) == 2
    assert id_one not in ids
    assert id_two in ids
    assert id_three in ids


def test_string_index(test_shard_double_index):
    shard, indexes = test_shard_double_index
    data_one = {'num': 4, 'related_id': None, 'text': 'first insert'}
    id_one = insert_test_obj(shard, data_one)
    data_two = {'num': 4, 'related_id': id_one, 'text': 'second insert'}
    id_two = insert_test_obj(shard, data_two)
    data_three = {'num': 5, 'related_id': id_one, 'text': 'third insert'}
    id_three = insert_test_obj(shard, data_three)

    index_name = 'num_index'
    num_index = indexes[index_name]
    sync_kv_insert_index_entry(shard, num_index, 4, id_one)
    sync_kv_insert_index_entry(shard, num_index, 4, id_two)
    sync_kv_insert_index_entry(shard, num_index, 5, id_three)

    four_ids = sync_kv_get_index_ids(shard, num_index, 4)

    assert id_one in four_ids
    assert id_two in four_ids
    assert id_three not in four_ids

    five_ids = sync_kv_get_index_ids(shard, num_index, 5)

    assert id_one not in five_ids
    assert id_two not in five_ids
    assert id_three in five_ids
