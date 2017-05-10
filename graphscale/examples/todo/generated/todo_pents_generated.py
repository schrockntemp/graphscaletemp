from uuid import UUID

from graphscale.pent.pent import Pent
from graphscale.grapple.grapple_utils import req_data_elem_invalid, req_data_elem_valid

class TodoUserGenerated(Pent):
    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(data):
        if not isinstance(data, dict):
            return False
        if req_data_elem_invalid(data, 'obj_id', UUID): # id: ID!
            return False
        if req_data_elem_invalid(data, 'name', str): # name: String!
            return False
        return True

    def name(self):
        return self._data['name']

    async def gen_todo_items(self, after=None, first=None):
        target_type = self.config().get_edge_target_type_from_name('user_to_todo_edge')
        return await self.gen_associated_pents(target_type, 'user_to_todo_edge', after, first)

class TodoItemGenerated(Pent):
    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(data):
        if not isinstance(data, dict):
            return False
        if not req_data_elem_valid(data, 'obj_id', UUID): # id: ID!
            return False
        if not req_data_elem_valid(data, 'text', str): # text: String!
            return False
        return True

    def text(self):
        return self._data['text']

    async def gen_user(self):
        klass = self.config().get_type(1000) # type_id
        return await klass.gen(self._context, self._data['user_id'])
