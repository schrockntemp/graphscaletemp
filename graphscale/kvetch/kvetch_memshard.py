from datetime import datetime
from uuid import UUID

from graphscale.utils import param_check

from graphscale.kvetch.kvetch import (
    KvetchShard,
    KvetchShardIndex,
)

class KvetchMemIndex(KvetchShardIndex):
    def __init__(self, *, indexed_attr, index_name):
        param_check(indexed_attr, str, 'indexed_attr')
        param_check(index_name, str, 'index_name')

        self._indexed_attr = indexed_attr
        self._index_name = index_name

    def index_name(self):
        return self._index_name

    def indexed_attr(self):
        return self._indexed_attr

class KvetchMemEdgeDefinition:
    def __init__(self, *, name):
        self._name = name

    def name(self):
        return self._name

def safe_append_to_dict_of_list(dict_of_list, key, value):
    if key not in dict_of_list:
        dict_of_list[key] = []
    dict_of_list[key].append(value)

class KvetchMemShard(KvetchShard):
    def __init__(self):
        self._objects = {}
        self._all_indexes = {}
        self._all_edges = {}

    async def gen_object(self, id_):
        param_check(id_, UUID, 'id_')
        return self._objects.get(id_)

    async def gen_objects(self, ids):
        param_check(ids, list, 'ids')
        if not ids:
            raise ValueError('ids must have at least 1 element')
        return {id_: self._objects.get(id_) for id_ in ids}

    async def gen_insert_index_entry(self, index, index_value, target_id):
        index_name = index.index_name()
        if index_name not in self._all_indexes:
            self._all_indexes[index_name] = {}

        index_dict = self._all_indexes[index_name]
        index_entry = {'target_id': target_id, 'updated': datetime.now()}
        safe_append_to_dict_of_list(index_dict, index_value, index_entry)
        return index_entry

    async def gen_index_entries(self, index, index_value):
        index_dict = self._all_indexes[index.index_name()]
        return index_dict[index_value]

    async def gen_insert_object(self, new_id, type_id, data):
        self.check_insert_object_vars(new_id, type_id, data)

        self._objects[new_id] = {
            **{'id': new_id, '__type_id': type_id, 'updated': datetime.now()},
            **data
        }
        return new_id

    async def gen_insert_edge(self, edge_definition, from_id, to_id, data=None):
        param_check(from_id, UUID, 'from_id')
        param_check(to_id, UUID, 'to_id')
        if data is None:
            data = {}
        param_check(data, dict, 'data')

        edge_name = edge_definition.name()
        if edge_name not in self._all_edges:
            self._all_edges[edge_name] = {}

        edge_entry = {'from_id': from_id, 'to_id': to_id, 'data': data}
        safe_append_to_dict_of_list(self._all_edges[edge_name], from_id, edge_entry)

    async def gen_edges(self, edge_definition, from_id):
        return self._all_edges[edge_definition.name()][from_id]

    async def gen_edge_ids(self, edge_definition, from_id):
        edges = self._all_edges[edge_definition.name()][from_id]
        return [edge['to_id'] for edge in edges]
