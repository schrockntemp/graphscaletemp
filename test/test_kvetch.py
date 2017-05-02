from uuid import UUID, uuid4

import pytest

from graphscale.kvetch import Kvetch
from graphscale.kvetch.kvetch_memshard import (
    KvetchMemShard,
    KvetchMemIndexDefinition,
    KvetchMemEdgeDefinition,
)

#W0621 display redefine variable for test fixture
#pylint: disable=W0621,C0103,W0401,W0614

def single_shard_no_index():
    shard = KvetchMemShard()
    return Kvetch(shards=[shard], edges=[], indexes=[])

def two_shards_no_index():
    shards = [KvetchMemShard() for i in range(0, 2)]
    return Kvetch(shards=shards, edges=[], indexes=[])

def three_shards_no_index():
    shards = [KvetchMemShard() for i in range(0, 3)]
    return Kvetch(shards=shards, edges=[], indexes=[])

def many_shards_no_index():
    shards = [KvetchMemShard() for i in range(0, 16)]
    return Kvetch(shards=shards, edges=[], indexes=[])

def single_shard_with_related_edge():
    related_edge = KvetchMemEdgeDefinition(
        edge_name='related_edge',
        edge_id=12345,
        from_id_attr='related_id'
    )
    return Kvetch(shards=[KvetchMemShard()], edges=[related_edge], indexes=[])

def many_shards_with_related_edge():
    related_edge = KvetchMemEdgeDefinition(
        edge_name='related_edge',
        edge_id=12345,
        from_id_attr='related_id'
    )
    shards = [KvetchMemShard() for i in range(0, 16)]
    return Kvetch(shards=shards, edges=[related_edge], indexes=[])

@pytest.fixture(params=[
    single_shard_no_index,
    two_shards_no_index,
    three_shards_no_index,
    many_shards_no_index])
def no_index_kvetch(request):
    return request.param()

@pytest.fixture(params=[single_shard_with_related_edge, many_shards_with_related_edge])
def single_edge_kvetch(request):
    return request.param()

def single_shard_single_index():
    num_index = KvetchMemIndexDefinition(indexed_attr='num', index_name='num_index')
    return Kvetch(shards=[KvetchMemShard()], edges=[], indexes=[num_index])

@pytest.fixture
def single_index_kvetch():
    return single_shard_single_index()

@pytest.mark.asyncio
async def test_insert_get(no_index_kvetch):
    new_id = await no_index_kvetch.gen_insert_object(1000, {'num' : 4})
    assert isinstance(new_id, UUID)
    new_obj = await no_index_kvetch.gen_object(new_id)
    assert new_obj['id'] == new_id
    assert new_obj['__type_id'] == 1000
    assert new_obj['num'] == 4

@pytest.mark.asyncio
async def test_insert_gen_objects():
    kvetch = many_shards_no_index()

    id_one = await kvetch.gen_insert_object(1000, {'num' : 4})
    id_two = await kvetch.gen_insert_object(1000, {'num' : 5})
    id_three = await kvetch.gen_insert_object(1000, {'num' : 6})

    obj_dict = await kvetch.gen_objects([id_one, id_two, id_three])
    assert len(obj_dict) == 3
    for data in obj_dict.values():
        del data['updated']

    expected = {
        id_one: {
            'id': id_one,
            '__type_id': 1000,
            'num': 4,
        },
        id_two: {
            'id': id_two,
            '__type_id': 1000,
            'num': 5,
        },
        id_three: {
            'id': id_three,
            '__type_id': 1000,
            'num': 6,
        },
    }
    assert obj_dict == expected

@pytest.mark.asyncio
async def test_many_objects_many_shards():
    kvetch = many_shards_no_index()
    ids = []
    id_to_num = {}

    for i in range(0, 100):
        new_id = await kvetch.gen_insert_object(234, {'num' : i})
        ids.append(new_id)
        id_to_num[new_id] = i

    count = 0
    for id_ in ids:
        obj = await kvetch.gen_object(id_)
        assert obj['id'] == id_
        assert obj['num'] == count
        assert obj['__type_id'] == 234
        count += 1

    objs = await kvetch.gen_objects(ids)
    for id_, obj in objs.items():
        assert obj['id'] == id_
        assert id_to_num[id_] == obj['num']

@pytest.mark.asyncio
async def test_single_edge_kvetch(single_edge_kvetch):
    kvetch = single_edge_kvetch
    type_id = 2345

    data_one = {'name': 'John', 'related_id': None}
    id_one = await kvetch.gen_insert_object(type_id, data_one)

    ids_related_to_one = []
    for i in range(0, 10):
        data_n = {'name': 'Jane' + str(i), 'related_id': id_one}
        ids_related_to_one.append(await kvetch.gen_insert_object(type_id, data_n))

    related_edge = kvetch.get_edge_definition_by_name('related_edge')
    edges = await kvetch.gen_edges(related_edge, id_one)
    id_results = [edge['to_id'] for edge in edges]
    assert set(ids_related_to_one) == set(id_results)

@pytest.mark.asyncio
async def test_single_index_kvetch(single_index_kvetch):
    kvetch = single_index_kvetch
    type_id = 2345
    ids_with_2 = []
    for i in range(0, 10):
        data_n = {'name': 'Jane' + str(i), 'num': 2}
        ids_with_2.append(await kvetch.gen_insert_object(type_id, data_n))

    ids_with_3 = []
    for i in range(0, 10):
        data_n = {'name': 'John' + str(i), 'num': 3}
        ids_with_3.append(await kvetch.gen_insert_object(type_id, data_n))

    num_index = kvetch.get_index('num_index')

    result_from_2 = await kvetch.gen_from_index(num_index, 2)
    assert set(ids_with_2) == set(result_from_2)

    result_from_3 = await kvetch.gen_from_index(num_index, 3)
    assert set(ids_with_3) == set(result_from_3)
