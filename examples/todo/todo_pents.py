# from graphscale.kvetch.kvetch_db import (
#     kv_gen_object,
#     kv_gen_objects,
#     kv_gen_index_entries,
#     kv_insert_object,
#     kv_insert_index_entry,
# )

from graphscale.kvetch.kvetch import (
    Kvetch,
)

from graphscale.utils import(
    param_check,
)

from uuid import(
    UUID,
)

def reverse_dict(dict_to_reverse):
    return {v: k for k, v in dict_to_reverse.items()}


# TODDO make this a context and have it contain a kvetch context
class PentConfig:
    def __init__(self, id_to_cls_dict):
        self._id_to_cls = id_to_cls_dict
        self._cls_to_id = reverse_dict(id_to_cls_dict)

    def get_type(self, type_id):
        return self._id_to_cls[type_id]

    def get_type_id(self, klass):
        return self._cls_to_id[klass]


async def load_pent(context, id_):
    data = await context.kvetch().gen_object(id_)
    cls = context.config().get_type(data['__type_id'])
    return cls(context, id_, data)

async def load_pents(context, ids):
    obj_dict = await context.kvetch().gen_objects(ids)
    pent_dict = {}
    for id_, data in obj_dict.items():
        cls = context.config().get_type(data['__type_id'])
        pent_dict[id_] = cls(context, id_, data)
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
        return cls(context, id_, data)

    @classmethod
    async def gen_list(cls, context, ids):
        if cls == Pent:
            return await load_pents(context, ids)
        data_list = await context.kvetch().gen_objects(ids)
        return [cls(context, data['id'], data) for data in data_list.values()]


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


# async def gen_pent_index_list(pent, index_name, klass, after, first):
#     entries = await kv_gen_index_entries(pent.context(), index_name, pent.id_())
#     ids = [entry['id_to'] for entry in entries]
#     return await klass.gen_list(pent.context(), ids)

# async def gen_pent_edge_ids(kvetch, edge_name, klass, after, first):
#     edges = kvetch.gen_edges(kvetch.get_edge)

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

class TodoUser(Pent):
    def name(self):
        return self._data['name']

    async def gen_todo_items(self, _after=None, _first=None):
        edges = await self.gen_edges_to('todo_to_user_edge')
        to_ids = [edge['to_id'] for edge in edges]
        return await TodoItem.gen_list(self._context, to_ids)

class TodoUserInput:
    def __init__(self, *, name):
        self._data = {
            'name' : name,
        }

    def data(self):
        return self._data

class TodoItemInput:
    def __init__(self, *, user_id, text):
        self._data = {
            'text' : text,
            'user_id' : user_id,
        }

    def data(self):
        return self._data

class TodoItem(Pent):
    def text(self):
        return self._data['text']

    async def gen_user(self):
        return await TodoUser.gen(self._context, self._data['user_id'])

async def create_todo_user(context, input_object):
    type_id = context.config().get_type_id(TodoUser)
    data = input_object.data()
    new_id = await context.kvetch().gen_insert_object(type_id, data)
    return await TodoUser.gen(context, new_id)

async def create_todo_item(context, input_object):
    type_id = context.config().get_type_id(TodoItem)
    data = input_object.data()
    new_id = await context.kvetch().gen_insert_object(type_id, data)
    return await TodoItem.gen(context, new_id)

def get_todo_type_id_class_map():
    return {
        1000 : TodoUser,
        1001 : TodoItem,
    }
