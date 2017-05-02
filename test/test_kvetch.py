from uuid import UUID, uuid4

import pytest

from graphscale.kvetch import Kvetch
from graphscale.kvetch.kvetch_memshard import (
    KvetchMemShard,
    KvetchMemIndex
)

#W0621 display redefine variable for test fixture
#pylint: disable=W0621,C0103,W0401,W0614

def single_shard_no_index():
    shard = KvetchMemShard()
    return Kvetch(shards=[shard], indexes=[])

def two_shards_no_index():
    shards = [KvetchMemShard() for i in range(0, 2)]
    return Kvetch(shards=shards, indexes=[])

def three_shards_no_index():
    shards = [KvetchMemShard() for i in range(0, 3)]
    return Kvetch(shards=shards, indexes=[])

def many_shards_no_index():
    shards = [KvetchMemShard() for i in range(0, 16)]
    return Kvetch(shards=shards, indexes=[])

def single_shard_with_related_index():
    related_index = KvetchMemIndex(
        indexed_attr='related_id',
        index_name='related_id_index',
    )
    return Kvetch(shards=[KvetchMemShard()], indexes=[related_index])

@pytest.fixture(params=[single_shard_no_index, two_shards_no_index, three_shards_no_index])
def test_kvetch_no_index(request):
    return request.param()

@pytest.mark.asyncio
async def test_insert_get(test_kvetch_no_index):
    new_id = await test_kvetch_no_index.gen_insert_object(1000, {'num' : 4})
    assert isinstance(new_id, UUID)
    new_obj = await test_kvetch_no_index.gen_object(new_id)
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
    test_kvetch = many_shards_no_index()
    ids = []
    id_to_num = {}

    for i in range(0, 100):
        new_id = await test_kvetch.gen_insert_object(234, {'num' : i})
        ids.append(new_id)
        id_to_num[new_id] = i

    count = 0
    for id_ in ids:
        obj = await test_kvetch.gen_object(id_)
        assert obj['id'] == id_
        assert obj['num'] == count
        assert obj['__type_id'] == 234
        count += 1

    objs = await test_kvetch.gen_objects(ids)
    for id_, obj in objs.items():
        assert obj['id'] == id_
        assert id_to_num[id_] == obj['num']

@pytest.mark.asyncio
async def test_single_shard_with_single_index():
    test_kvetch = single_shard_with_related_index()
    type_id = 2345

    data_one = {'name': 'John', 'related_id': None}
    id_one = test_kvetch.gen_insert_object(type_id, data_one)

    ids_related_to_one = []
    for i in range(0, 10):
        data_n = {'name': 'Jane' + str(i), 'related_id': id_one}
        ids_related_to_one.append(test_kvetch.gen_insert_object(type_id, data_n))

    related_index = test_kvetch.get_index('related_id_index')
    id_results = await test_kvetch.gen_all(related_index, id_one)

# TODO test shard logic for index
