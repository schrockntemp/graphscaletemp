from graphscale.pent.pent import PentConfig, create_pent
from graphscale.utils import param_check

from .generated.todo_pents_generated import TodoUserGenerated, TodoItemGenerated

class TodoUser(TodoUserGenerated):
    pass

class TodoUserInput:
    def __init__(self, *, name):
        self._data = {
            'name' : name,
        }

    def data(self):
        return self._data

class TodoItem(TodoItemGenerated):
    pass

class TodoItemInput:
    def __init__(self, *, user_id, text):
        self._data = {
            'text' : text,
            'user_id' : user_id,
        }

    def data(self):
        return self._data

async def create_todo_user(context, input_object):
    param_check(input_object, TodoUserInput, 'input_object')
    return await create_pent(context, TodoUser, input_object)

async def create_todo_item(context, input_object):
    param_check(input_object, TodoItemInput, 'input_object')
    return await create_pent(context, TodoItem, input_object)

def get_object_config():
    return {
        1000 : TodoUser,
        1001 : TodoItem,
    }

def get_edge_config():
    return {
        9283 : {'edge_name': 'user_to_todo_edge', 'target_type': TodoItem}
    }

def get_todo_config():
    return PentConfig(object_config=get_object_config(), edge_config=get_edge_config())
