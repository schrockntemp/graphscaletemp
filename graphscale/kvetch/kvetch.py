from uuid import UUID, uuid4
from graphscale.utils import param_check, async_array

class KvetchShard:
    def check_insert_object_vars(self, new_id, type_id, data):
        param_check(new_id, UUID, 'new_id')
        param_check(type_id, int, 'type_id')
        param_check(data, dict, 'data')
        if 'obj_id' in data:
            raise ValueError('Cannot specify obj_id')

        if 'type_id' in data:
            raise ValueError('Cannot specify type_id')

class KvetchIndexDefinition:
    async def gen_all(self, _shard, _value):
        raise Exception('must implement')

class KvetchEdgeDefinition:
    def __init__(self, *, edge_name, edge_id, from_id_attr):
        self._edge_name = edge_name
        self._edge_id = edge_id
        self._from_id_attr = from_id_attr

    def edge_name(self):
        return self._edge_name

    def edge_id(self):
        return self._edge_id

    def from_id_attr(self):
        return self._from_id_attr

class Kvetch:
    def __init__(self, *, shards, edges, indexes):
        param_check(shards, list, 'shards')
        param_check(edges, list, 'edges')
        param_check(indexes, list, 'indexes')

        self._shards = shards
        # shard => shard_id
        self._shard_lookup = dict(zip(self._shards, range(0, len(shards))))
        # index_name => index
        self._index_dict = dict(zip([index.index_name() for index in indexes], indexes))
        self._edge_dict = dict(zip([edge.edge_name() for edge in edges], edges))

    def get_index(self, index_name):
        param_check(index_name, str, 'index_name')
        return self._index_dict[index_name]

    def get_edge_definition_by_name(self, edge_name):
        return next(edge for edge in self._edge_dict.values() if edge.edge_name() == edge_name)

    def get_shard_from_obj_id(self, obj_id):
        param_check(obj_id, UUID, 'obj_id')
        shard_id = self.get_shard_id_from_obj_id(obj_id)
        return self._shards[shard_id]

    def get_shard_id_from_obj_id(self, obj_id):
        # do something less stupid like consistent hashing
        # excellent description here http://michaelnielsen.org/blog/consistent-hashing/
        param_check(obj_id, UUID, 'obj_id')
        return int(obj_id) % len(self._shards)

    async def gen_update_object(self, obj_id, data):
        param_check(obj_id, UUID, 'obj_id')
        param_check(data, dict, 'data')

        shard = self.get_shard_from_obj_id(obj_id)
        return await shard.gen_update_object(obj_id, data)

    async def gen_delete_object(self, obj_id):
        param_check(obj_id, UUID, 'obj_id')
        shard = self.get_shard_from_obj_id(obj_id)
        return await shard.gen_delete_object(obj_id)

    async def gen_insert_object(self, type_id, data):
        param_check(type_id, int, 'type_id')
        param_check(data, dict, 'data')

        new_id = uuid4()
        shard = self.get_shard_from_obj_id(new_id)
        await shard.gen_insert_object(new_id, type_id, data)

        for edge_definition in self._edge_dict.values():
            attr = edge_definition.from_id_attr()
            if not(attr in data) or not data[attr]:
                continue

            from_id = data[attr]
            from_id_shard = self.get_shard_from_obj_id(from_id)
            await from_id_shard.gen_insert_edge(edge_definition, from_id, new_id, {})

        for index in self._index_dict.values():
            attr = index.indexed_attr()
            if not(attr in data) or not data[attr]:
                continue

            indexed_value = data[attr]
            indexed_shard = self.get_shard_from_obj_id(new_id)
            await indexed_shard.gen_insert_index_entry(index, indexed_value, new_id)

        return new_id

    async def gen_object(self, obj_id):
        param_check(obj_id, UUID, 'obj_id')
        shard = self.get_shard_from_obj_id(obj_id)
        return await shard.gen_object(obj_id)

    async def gen_objects(self, ids):
        # construct dictionary of shard_id to all ids in that shard
        shard_to_ids = {} # shard_id => [id]
        for obj_id in ids:
            shard_id = self.get_shard_id_from_obj_id(obj_id)
            if not shard_id in shard_to_ids:
                shard_to_ids[shard_id] = []
            shard_to_ids[shard_id].append(obj_id)

        # construct list of coros (one per shard) in order to fetch in parallel
        unawaited_gens = []
        for shard_id, ids_in_shard in shard_to_ids.items():
            shard = self._shards[shard_id]
            unawaited_gens.append(shard.gen_objects(ids_in_shard))

        obj_dict_per_shard = await async_array(unawaited_gens)

        # flatten results into single dict
        results = {}
        for obj_dict in obj_dict_per_shard:
            for obj_id, obj in obj_dict.items():
                results[obj_id] = obj
        return results

    async def gen_objects_of_type(self, type_id, after=None, first=None):
        if len(self._shards) > 1:
            raise Exception('shards > 1 currently not supported')

        shard = self._shards[0]
        return await shard.gen_objects_of_type(type_id, after, first)

    async def gen_edges(self, edge_definition, from_id, after=None, first=None):
        shard = self.get_shard_from_obj_id(from_id)
        return await shard.gen_edges(edge_definition, from_id, after=after, first=first)

    async def gen_from_index(self, index, index_value):
        ids = []
        for shard in self._shards:
            index_entries = await shard.gen_index_entries(index, index_value)
            ids.extend([entry['target_id'] for entry in index_entries])

        return await self.gen_objects(ids)
