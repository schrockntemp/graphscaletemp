from uuid import UUID

from graphscale.kvetch.kvetch import Kvetch
from graphscale.utils import param_check


def reverse_dict(dict_to_reverse):
    return {v: k for k, v in dict_to_reverse.items()}

def safe_create(context, id_, klass, data):
    if not klass.is_db_data_valid(data):
        return None
    return klass(context, id_, data)

class PentConfig:
    def __init__(self, id_to_klass_dict):
        self._id_to_klass = id_to_klass_dict
        self._klass_to_id = reverse_dict(id_to_klass_dict)

    def get_type(self, type_id):
        return self._id_to_klass[type_id]

    def get_type_id(self, klass):
        return self._klass_to_id[klass]

async def load_pent(context, id_):
    data = await context.kvetch().gen_object(id_)
    klass = context.config().get_type(data['__type_id'])
    return safe_create(context, id_, klass, data)

async def load_pents(context, ids):
    obj_dict = await context.kvetch().gen_objects(ids)
    pent_dict = {}
    for id_, data in obj_dict.items():
        klass = context.config().get_type(data['__type_id'])
        pent_dict[id_] = safe_create(context, id_, klass, data)
    return pent_dict

class Pent:
    def __init__(self, context, id_, data):
        param_check(context, PentContext, 'context')
        param_check(id_, UUID, 'id_')
        param_check(data, dict, 'dict')

        self._context = context
        self._id = id_
        self._data = data

    def kvetch(self):
        return self._context.kvetch()

    @classmethod
    async def gen(cls, context, id_):
        if cls == Pent:
            return await load_pent(context, id_)

        data = await context.kvetch().gen_object(id_)
        return safe_create(context, id_, cls, data)

    @classmethod
    async def gen_list(cls, context, ids):
        if cls == Pent:
            return await load_pents(context, ids)
        data_list = await context.kvetch().gen_objects(ids)
        return [safe_create(context, data['id'], cls, data) for data in data_list.values()]


    def id_(self):
        return self._id

    def context(self):
        return self._context

    def data(self):
        return self._data

    async def gen_edges_to(self, edge_name, after=None, first=None):
        if after is not None or first is not None:
            raise Exception('after, from not supported after: %s first: %s' % (after, first))

        kvetch = self.kvetch()

        edge_definition = kvetch.get_edge_definition_by_name(edge_name)
        edges = await kvetch.gen_edges(edge_definition, self.id_())
        return edges

    async def gen_associated_pents(self, klass, edge_name, after=None, first=None):
        if after is not None or first is not None:
            raise Exception('after, from not supported after: %s first: %s' % (after, first))
        edges = await self.gen_edges_to(edge_name)
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
