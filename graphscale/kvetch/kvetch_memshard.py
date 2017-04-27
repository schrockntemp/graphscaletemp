from datetime import datetime
from uuid import UUID, uuid4

from graphscale.utils import param_check

from graphscale.kvetch.kvetch import (
    KvetchShard,
    KvetchShardIndex,
)

class KvetchMemShardIndex(KvetchShardIndex):
    def __init__(self, *, indexed_attr, index_name):
        param_check(indexed_attr, str, 'indexed_attr')
        param_check(index_name, str, 'index_name')

        self._indexed_attr = indexed_attr
        self._index_name = index_name

    def index_name(self):
        return self._index_name

    def indexed_attr(self):
        return self._indexed_attr

    async def gen_all(self, shard, value):
        raise Exception('todo')
        # entries = _kv_shard_get_index_entries(
        #     shard_conn=shard.conn(),
        #     index_name=self.index_name(),
        #     index_column=self.indexed_attr(),
        #     index_value=value,
        #     target_column='entity_id',
        # )
        # ids = [UUID(bytes=entry['target_value']) for entry in entries]
        # objs = await shard.gen_objects(ids)
        # return objs

class KvetchMemShard(KvetchShard):
    def __init__(self, *, indexes):
        self._index_dict = dict(zip([index.index_name() for index in indexes], indexes))
        self._objects = {}
        self._indexes = {index.index_name() : {} for index in indexes}

    def indexes(self):
        return self._index_dict.values()

    def index_by_name(self, name):
        return self._index_dict[name]

    async def gen_object(self, id_):
        param_check(id_, UUID, 'id_')
        return self._objects.get(id_)

    async def gen_objects(self, ids):
        param_check(ids, list, 'ids')
        if not ids:
            raise ValueError('ids must have at least 1 element')
        return {id_: self._objects.get(id_) for id_ in ids}

    async def gen_insert_object(self, type_id, data):
        self.check_insert_object_vars(type_id, data)
        new_id = uuid4()
        self._objects[new_id] = {
            **{'id' : new_id, '__type_id' : type_id, 'updated' : datetime.now()},
            **data
        }

        def safe_append_to_dict_list(dict_list, key, value):
            if key not in dict_list:
                dict_list[key] = []
            dict_list[key].append(value)

        for index_name, index in self._index_dict.items():
            attr = index.indexed_attr()
            if attr in data and data[attr]:
                index_dict = self._indexes[index_name]
                index_value = data[attr]
                # { index_name => { index_value => { target_value: , updated } } }
                safe_append_to_dict_list(index_dict, index_value, {
                    'target_value' : new_id,
                    'updated' : datetime.now()
                })

        return new_id

    async def gen_all(self, index, value):
        index_data = self._indexes[index.index_name()]
        ids = [entry['target_value'] for entry in index_data[value]]
        return await self.gen_objects(ids)
