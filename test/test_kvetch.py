from uuid import UUID, uuid4

import pytest

from graphscale.kvetch import Kvetch
from graphscale.kvetch.kvetch_memshard import (
    KvetchMemShard,
    KvetchMemIndex
)

class MockReturns:
    def __init__(self, return_values):
        self._return_values = return_values
        self._call_num = 0

    def call_me(self):
        return_value = self._return_values[self._call_num]
        self._call_num += 1
        return return_value

def single_shard_kvetch():
    shard = KvetchMemShard(indexes=[])
    return Kvetch(shards=[shard], indexes=[])

def two_shards_kvetch():
    shards = [KvetchMemShard(indexes=[]) for i in range(0, 2)]
    return Kvetch(shards=shards, indexes=[])

def three_shards_kvetch():
    shards = [KvetchMemShard(indexes=[]) for i in range(0, 3)]
    return Kvetch(shards=shards, indexes=[])

def many_shards_kvetch():
    shards = [KvetchMemShard(indexes=[]) for i in range(0, 16)]
    return Kvetch(shards=shards, indexes=[])

@pytest.mark.asyncio
async def test_insert_get_single_shard():
    test_kvetch = single_shard_kvetch()
    await do_test_insert_get(test_kvetch)

@pytest.mark.asyncio
async def test_insert_get_two_shard():
    test_kvetch = two_shards_kvetch()
    await do_test_insert_get(test_kvetch)

@pytest.mark.asyncio
async def test_insert_get_two_shard_zero():
    test_kvetch = two_shards_kvetch()
    test_kvetch.get_shard_id = lambda _id: 0
    await do_test_insert_get(test_kvetch)

@pytest.mark.asyncio
async def test_insert_get_two_shard_one():
    test_kvetch = two_shards_kvetch()
    test_kvetch.get_shard_id = lambda _id: 1
    await do_test_insert_get(test_kvetch)

@pytest.mark.asyncio
async def test_insert_get_three_shard():
    test_kvetch = three_shards_kvetch()
    await do_test_insert_get(test_kvetch)

@pytest.mark.asyncio
async def do_test_insert_get(test_kvetch):
    new_id = await test_kvetch.gen_insert_object(1000, {'num' : 4})
    assert isinstance(new_id, UUID)
    new_obj = await test_kvetch.gen_object(new_id)
    assert new_obj['id'] == new_id
    assert new_obj['__type_id'] == 1000
    assert new_obj['num'] == 4

@pytest.mark.asyncio
async def test_many_objects_many_shards():
    test_kvetch = many_shards_kvetch()
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
