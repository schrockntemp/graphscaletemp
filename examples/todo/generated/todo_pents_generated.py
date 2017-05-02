from uuid import UUID

from graphscale.pent.pent import Pent

def data_elem_valid(data, key, klass):
    return (key in data) and data[key] and isinstance(data[key], klass)

class TodoUserGenerated(Pent):
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

    async def gen_todo_items(self, after=None, first=None):
        target_type = self.config().get_edge_target_type_from_name('user_to_todo_edge')
        return await self.gen_associated_pents(target_type, 'user_to_todo_edge', after, first)

class TodoItemGenerated(Pent):
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
        klass = self.config().get_type(1000) #type_id
        return await klass.gen(self._context, self._data['user_id'])
