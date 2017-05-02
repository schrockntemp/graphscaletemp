from uuid import UUID, uuid4

#W0621 display redefine variable for test fixture
#pylint: disable=W0621,C0103,W0401,W0614

import pytest

from graphscale.kvetch.kvetch_dbshard import (
    KvetchDbShard,
    KvetchDbIndexDefinition,
    KvetchDbEdgeDefinition,
    KvetchDbSingleConnectionPool,
    sync_kv_insert_object,
    sync_kv_get_object,
    sync_kv_get_objects,
    sync_kv_insert_index_entry,
    sync_kv_get_index_ids,
    sync_kv_insert_edge,
    sync_kv_get_edge_ids,
)

from graphscale.kvetch.kvetch_memshard import (
    KvetchMemShard,
    KvetchMemIndexDefinition,
    KvetchMemEdgeDefinition,
)

from graphscale.kvetch.kvetch_dbschema import (
    drop_shard_db_tables,
    init_shard_db_tables,
)

from .test_utils import MagnusConn

def mem_single_edge_shard():
    related_edge = KvetchMemEdgeDefinition(
        edge_name='related_edge',
        edge_id=12345,
        from_id_attr='related_id',
    )
    edges = {
        'related_edge': related_edge,
    }
    return (KvetchMemShard(), edges, {})


def db_single_edge_shard():
    related_edge = KvetchDbEdgeDefinition(
        edge_name='related_edge',
        edge_id=12345,
        from_id_attr='related_id',
    )
    shard = KvetchDbShard(
        pool=KvetchDbSingleConnectionPool(MagnusConn.get_conn()),
    )
    indexes = {}
    edges = {
        'related_edge' : related_edge,
    }
    drop_shard_db_tables(shard, indexes)
    init_shard_db_tables(shard, indexes)
    return (shard, edges, indexes)

@pytest.fixture
def only_shard():
    shard, _, __ = test_shard_single_edge()
    return shard

@pytest.fixture
def test_shard_single_edge():
    return mem_single_edge_shard()
    # return db_single_edge_shard()

@pytest.fixture
def test_shard_single_index():
    return mem_edge_and_index_shard()
    # return db_edge_and_index_shard()

def mem_edge_and_index_shard():
    related_edge = KvetchMemEdgeDefinition(
        edge_name='related_edge',
        edge_id=12345,
        from_id_attr='related_id',
    )
    num_index = KvetchMemIndexDefinition(
        indexed_attr='num',
        index_name='num_index'
    )
    edges = {'related_edge': related_edge}
    indexes = {'num_index': num_index}
    return (KvetchMemShard(), edges, indexes)

def db_edge_and_index_shard():
    related_edge = KvetchDbEdgeDefinition(
        edge_name='related_edge',
        edge_id=12345,
        from_id_attr='related_id'
    )
    num_index = KvetchDbIndexDefinition(
        indexed_attr='num',
        indexed_sql_type='INT',
        index_name='num_index'
    )
    shard = KvetchDbShard(
        pool=KvetchDbSingleConnectionPool(MagnusConn.get_conn()),
    )
    edges = {'related_edge': related_edge}
    indexes = {'num_index': num_index}
    drop_shard_db_tables(shard, indexes)
    init_shard_db_tables(shard, indexes)
    return shard, edges, indexes

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
    id_two = insert_test_obj(shard, data_two)
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


def test_id_edge(test_shard_single_edge):
    shard, edges, _ = test_shard_single_edge
    data_one = {'num': 4}
    id_one = insert_test_obj(shard, data_one)
    data_two = {'num': 5, 'related_id': id_one}
    id_two = insert_test_obj(shard, data_two)
    data_three = {'num': 6, 'related_id': id_one}
    id_three = insert_test_obj(shard, data_three)

    edge_name = 'related_edge'
    related_edge = edges[edge_name]
    assert related_edge is not None
    sync_kv_insert_edge(shard, related_edge, id_one, id_two)
    sync_kv_insert_edge(shard, related_edge, id_one, id_three)
    ids = sync_kv_get_edge_ids(shard, related_edge, id_one)
    assert len(ids) == 2
    assert id_one not in ids
    assert id_two in ids
    assert id_three in ids


def test_string_index(test_shard_single_index):
    shard, _, indexes = test_shard_single_index
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
