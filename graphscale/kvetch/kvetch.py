from uuid import UUID, uuid4
from graphscale.utils import param_check, async_array

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
    async def gen_all(self, shard, value):
        raise Exception('must implement')

class Kvetch:
    def __init__(self, shards):
        self._shards = shards
        self._shard_lookup = dict(zip(self._shards, range(0, len(shards))))

    def get_shard(self, id_):
        param_check(id_, UUID, 'id_')
        shard_id = self.get_shard_id(id_)
        return self._shards[shard_id]

    def get_shard_id(self, id_):
        # do something less stupid like consistent hashing
        # excellent description here http://michaelnielsen.org/blog/consistent-hashing/
        param_check(id_, UUID, 'id_')
        return int(id_) % len(self._shards)

    async def gen_insert_object(self, type_id, data):
        param_check(type_id, int, 'type_id')
        param_check(data, dict, 'data')
        new_id = uuid4()
        return await self.get_shard(new_id).gen_insert_object(new_id, type_id, data)

    async def gen_object(self, id_):
        param_check(id_, UUID, 'id_')
        shard = self.get_shard(id_)
        # print('id: ' + str(id_))
        # print('shard ' + str(self._shard_lookup[shard]))
        # print('objects: ' + str(shard._objects))
        return await shard.gen_object(id_)

    # not tested yet
    async def gen_objects(self, ids):
        shard_to_ids = {} # shard_id => [id]
        for id_ in ids:
            shard_id = self.get_shard_id(id_)
            if not shard_id in shard_to_ids:
                shard_to_ids[shard_id] = []
            shard_to_ids[shard_id].append(id_)

        coros = []
        for shard_id, ids_in_shard in shard_to_ids.items():
            shard = self._shards[shard_id]
            coros.append(shard.gen_objects(ids_in_shard))

        results = {}
        obj_dict_per_shard = await async_array(coros)
        for obj_dict in obj_dict_per_shard:
            for id_, obj in obj_dict.items():
                results[id_] = obj
        return results

