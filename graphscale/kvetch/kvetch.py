from enum import auto, Enum
from uuid import UUID, uuid4
from graphscale.utils import param_check, async_array

class ShardStrategyEnum(Enum):
    INDEXED_VALUE = auto() # example id => id one to many relation. searches single shard
    TARGET_ID = auto() # example. str => id. searches across all shards

class KvetchShard:
    def check_insert_object_vars(self, new_id, type_id, data):
        param_check(new_id, UUID, 'new_id')
        param_check(type_id, int, 'type_id')
        param_check(data, dict, 'data')
        if 'id' in data:
            raise ValueError('Cannot specify id')

        if '__type_id' in data:
            raise ValueError('Cannot specify __type_id')


class KvetchShardIndex:
    async def gen_all(self, _shard, _value):
        raise Exception('must implement')

class KvetchEdgeTable:
    pass

class Kvetch:
    def __init__(self, *, shards, indexes):
        self._shards = shards
        # shard => shard_id
        self._shard_lookup = dict(zip(self._shards, range(0, len(shards))))
        # index_name => index
        self._index_dict = dict(zip([index.index_name() for index in indexes], indexes))

    def get_index(self, index_name):
        param_check(index_name, str, 'index_name')
        return self._index_dict[index_name]

    def get_shard_from_obj_id(self, obj_id):
        param_check(obj_id, UUID, 'obj_id')
        shard_id = self.get_shard_id_from_obj_id(obj_id)
        return self._shards[shard_id]

    def get_shard_id_from_obj_id(self, obj_id):
        # do something less stupid like consistent hashing
        # excellent description here http://michaelnielsen.org/blog/consistent-hashing/
        param_check(obj_id, UUID, 'obj_id')
        return int(obj_id) % len(self._shards)

    def create_new_id(self): # seperate methdo in order to stub out in tests
        return uuid4()

    async def gen_insert_object(self, type_id, data):
        param_check(type_id, int, 'type_id')
        param_check(data, dict, 'data')
        new_id = self.create_new_id()

        shard = self.get_shard_from_obj_id(new_id)
        await shard.gen_insert_object(new_id, type_id, data)

        for index_name, index in self._index_dict.items():
            attr = index.indexed_attr()
            if not(attr in data) or data[attr]:
                continue

            # TODDO select correct shard with shard_on variable incorporatd
            indexed_id = data[attr]
            indexed_shard = self.get_shard_from_obj_id(indexed_id)
            await indexed_shard.gen_insert_index_entry(index_name, indexed_id, new_id)
            # actually insert the data now

        return new_id

    async def gen_object(self, id_):
        param_check(id_, UUID, 'id_')
        shard = self.get_shard_from_obj_id(id_)
        return await shard.gen_object(id_)

    async def gen_objects(self, ids):
        # construct dictionary of shard_id to all ids in that shard
        shard_to_ids = {} # shard_id => [id]
        for id_ in ids:
            shard_id = self.get_shard_id_from_obj_id(id_)
            if not shard_id in shard_to_ids:
                shard_to_ids[shard_id] = []
            shard_to_ids[shard_id].append(id_)

        # construct list of coros (one per shard) in order to fetch in parallel
        coros = []
        for shard_id, ids_in_shard in shard_to_ids.items():
            shard = self._shards[shard_id]
            coros.append(shard.gen_objects(ids_in_shard))

        obj_dict_per_shard = await async_array(coros)

        # flatten results into single dict
        results = {}
        for obj_dict in obj_dict_per_shard:
            for id_, obj in obj_dict.items():
                results[id_] = obj
        return results

    async def gen_all(self, index, from_value):
        pass 
        # if index.shard_strategy() == ShardStrategyEnum.INDEXED_VALUE:
        #     if not isinstance(from_value, UUID):
        #         raise Exception('for target id strategy value must be UUID')
        #     target_id = from_value
        #     shard = self.get_shard_from_obj_id(target_id)
        #     index_entries = await shard.gen_index_entries(self, target_id)
        #     ids = [index_entry['target_id'] for index_entry in index_entries]
        #     return await shard.gen_objects(ids)
        # elif index.shard_strategy() == ShardStrategyEnum.TARGET_ID:
        #     raise Exception('TARGET_ID not implemented yet')
        # else:
        #     raise Exception('strategy not supported: ' + str(index.shard_strategy()))
