from uuid import UUID

from graphscale.pent.pent import Pent
from graphscale.utils import param_check

def data_elem_valid(data, key, klass):
    return (key in data) and data[key] and isinstance(data[key], klass)

class TodoUser(Pent):
    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_db_data_valid(data):
        if not isinstance(data, dict):
            return False
        if not data_elem_valid(data, 'id', UUID): # id: ID!
            return False
        if not data_elem_valid(data, 'name', str): # name: String!
            return False
        return True

    def name(self):
        return self._data['name']

    async def gen_todo_items(self, _after=None, _first=None):
        return await self.gen_associated_pents(TodoItem, 'user_to_todo_edge')

class TodoUserInput:
    def __init__(self, *, name):
        self._data = {
            'name' : name,
        }

    def data(self):
        return self._data

class TodoItem(Pent):
    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_db_data_valid(data):
        if not isinstance(data, dict):
            return False
        if not data_elem_valid(data, 'id', UUID): # id: ID!
            return False
        if not data_elem_valid(data, 'text', str): # text: String!
            return False
        return True

    def text(self):
        return self._data['text']

    async def gen_user(self):
        return await TodoUser.gen(self._context, self._data['user_id'])

class TodoItemInput:
    def __init__(self, *, user_id, text):
        self._data = {
            'text' : text,
            'user_id' : user_id,
        }

    def data(self):
        return self._data

async def create_pent(context, klass, input_object):
    type_id = context.config().get_type_id(klass)
    data = input_object.data()
    new_id = await context.kvetch().gen_insert_object(type_id, data)
    return await klass.gen(context, new_id)

async def create_todo_user(context, input_object):
    param_check(input_object, TodoUserInput, 'input_object')
    return await create_pent(context, TodoUser, input_object)

async def create_todo_item(context, input_object):
    param_check(input_object, TodoItemInput, 'input_object')
    return await create_pent(context, TodoItem, input_object)

def get_todo_type_id_class_map():
    return {
        1000 : TodoUser,
        1001 : TodoItem,
    }
