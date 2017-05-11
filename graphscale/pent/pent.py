from uuid import UUID

from graphscale.kvetch.kvetch import Kvetch
from graphscale.utils import param_check

def reverse_dict(dict_to_reverse):
    return {v: k for k, v in dict_to_reverse.items()}

def safe_create(context, obj_id, klass, data):
    if not klass.is_input_data_valid(data):
        return None
    return klass(context, obj_id, data)

class PentConfig:
    def __init__(self, *, object_config, edge_config):
        self._object_config = object_config
        self._klass_to_id = reverse_dict(object_config)
        self._edge_config = edge_config
        self._name_to_edge = {edge['edge_name']: edge for edge_id, edge in edge_config.items()}

    def get_type(self, type_id):
        return self._object_config[type_id]

    def get_type_id(self, klass):
        return self._klass_to_id[klass]

    def get_edge_target_type_from_name(self, edge_name):
        return self._name_to_edge[edge_name]['target_type']

async def load_pent(context, obj_id):
    data = await context.kvetch().gen_object(obj_id)
    klass = context.config().get_type(data['type_id'])
    return safe_create(context, obj_id, klass, data)

async def load_pents(context, ids):
    obj_dict = await context.kvetch().gen_objects(ids)
    pent_dict = {}
    for obj_id, data in obj_dict.items():
        klass = context.config().get_type(data['type_id'])
        pent_dict[obj_id] = safe_create(context, obj_id, klass, data)
    return pent_dict

async def create_pent(context, klass, input_object):
    param_check(context, PentContext, 'context')
    type_id = context.config().get_type_id(klass)
    data = input_object.data()
    new_id = await context.kvetch().gen_insert_object(type_id, data)
    return await klass.gen(context, new_id)

async def update_pent(context, klass, obj_id, input_object):
    param_check(context, PentContext, 'context')
    data = input_object.data()
    await context.kvetch().gen_update_object(obj_id, data)
    return await klass.gen(context, obj_id)

async def delete_pent(context, _klass, obj_id):
    param_check(context, PentContext, 'context')
    await context.kvetch().gen_delete_object(obj_id)

class Pent:
    def __init__(self, context, obj_id, data):
        param_check(context, PentContext, 'context')
        param_check(obj_id, UUID, 'obj_id')
        param_check(data, dict, 'dict')

        self._context = context
        self._obj_id = obj_id
        self._data = data

    def kvetch(self):
        return self._context.kvetch()

    def config(self):
        return self._context.config()

    @classmethod
    async def gen(cls, context, obj_id):
        if cls == Pent:
            return await load_pent(context, obj_id)

        import sys
        sys.stderr.write('WORKS\n')
        data = await context.kvetch().gen_object(obj_id)
        if data is None:
            return None
        sys.stderr.write(str(data)+'\n')
        return safe_create(context, obj_id, cls, data)

    @classmethod
    async def gen_list(cls, context, ids):
        if cls == Pent:
            return await load_pents(context, ids)
        data_list = await context.kvetch().gen_objects(ids)
        return [safe_create(context, data['obj_id'], cls, data) for data in data_list.values()]

    @classmethod
    async def gen_all(cls, context, after, first):
        if cls == Pent:
            raise Exception('must specify concrete class')

        type_id = context.config().get_type_id(cls)
        data_list = await context.kvetch().gen_objects_of_type(type_id, after, first)
        return [safe_create(context, data['obj_id'], cls, data) for data in data_list.values()]

    def obj_id(self):
        return self._obj_id

    def context(self):
        return self._context

    def data(self):
        return self._data

    async def gen_edges_to(self, edge_name, after=None, first=None):
        kvetch = self.kvetch()

        edge_definition = kvetch.get_edge_definition_by_name(edge_name)
        edges = await kvetch.gen_edges(edge_definition, self.obj_id(), after=after, first=first)
        return edges

    async def gen_associated_pents(self, klass, edge_name, after=None, first=None):
        edges = await self.gen_edges_to(edge_name, after=after, first=first)
        to_ids = [edge['to_id'] for edge in edges]
        return await klass.gen_list(self._context, to_ids)


class PentContext:
    def __init__(self, *, kvetch, config):
        param_check(kvetch, Kvetch, 'kvetch')
        param_check(config, PentConfig, 'config')
        self._kvetch = kvetch
        self._config = config

    def kvetch(self):
        return self._kvetch

    def config(self):
        return self._config
