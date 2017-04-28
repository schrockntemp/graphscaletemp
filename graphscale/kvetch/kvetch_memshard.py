from datetime import datetime
from uuid import UUID

from graphscale.utils import param_check

from graphscale.kvetch.kvetch import (
    KvetchShard,
    KvetchShardIndex,
)


class KvetchMemIndex(KvetchShardIndex):
    def __init__(self, *, indexed_attr, index_name, shard_on=None):
        param_check(indexed_attr, str, 'indexed_attr')
        param_check(index_name, str, 'index_name')

        self._indexed_attr = indexed_attr
        self._index_name = index_name
        if shard_on is None:
            shard_on = indexed_attr
        self._shard_on = shard_on

    def index_name(self):
        return self._index_name

    def indexed_attr(self):
        return self._indexed_attr

    def shard_on(self):
        return self._shard_on


def safe_dict_get(d, key, default):
    if key not in d:
        d[key] = default
    return d[key]

def safe_append_to_dict_list(dict_list, key, value):
    if key not in dict_list:
        dict_list[key] = []
    dict_list[key].append(value)


class KvetchMemShard(KvetchShard):
    def __init__(self):
        self._objects = {}
        self._indexes = {}

    async def gen_object(self, id_):
        param_check(id_, UUID, 'id_')
        return self._objects.get(id_)

    async def gen_objects(self, ids):
        param_check(ids, list, 'ids')
        if not ids:
            raise ValueError('ids must have at least 1 element')
        return {id_: self._objects.get(id_) for id_ in ids}

    async def gen_insert_index_entry(self, index, index_value, target_value):
        index_name = index.index_name()
        index_dict = safe_dict_get(self._indexes, index_name, {})
        index_entry = {'target_value': target_value, 'updated': datetime.now()}
        safe_append_to_dict_list(index_dict, index_value, index_entry)
        return index_entry

    async def gen_index_entries(self, index, index_value):
        index_dict = self._indexes[index.index_name()]
        return index_dict[index_value]

    async def gen_insert_object(self, new_id, type_id, data):
        self.check_insert_object_vars(new_id, type_id, data)

        self._objects[new_id] = {
            **{'id': new_id, '__type_id': type_id, 'updated': datetime.now()},
            **data
        }
        return new_id

    # async def gen_all(self, index, value):
    #     index_data = self._indexes[index.index_name()]
    #     ids = [entry['target_value'] for entry in index_data[value]]
    #     return await self.gen_objects(ids)
