from datetime import datetime
from uuid import UUID, uuid4

from graphscale.utils import param_check

class KvetchMemShard:
    def __init__(self, *, indexes):
        self._index_dict = dict(zip([index.index_name() for index in indexes], indexes))
        self._objects = {}
        self._indexes = {index.index_name() : {} for index in indexes}

    def indexes(self):
        return self._index_dict.values()

    def index_by_name(self, name):
        return self._index_dict[name]

    async def gen_object(self, id_):
        param_check(id_, UUID, 'id_')
        return self._objects.get(id_)

    async def gen_objects(self, ids):
        param_check(ids, list, 'ids')
        if not ids:
            raise ValueError('ids must have at least 1 element')
        return {id_: self._objects.get(id_) for id_ in ids}

    async def gen_insert_object(self, type_id, data):
        new_id = uuid4()
        self._objects[new_id] = {
            **{'id' : new_id, '__type_id' : type_id, 'updated' : datetime.now()},
            **data
        }

        for index_name, index in self._index_dict.items():
            attr = index.indexed_attr()
            if attr in data and data[attr]:
                self._indexes[index_name][data[attr]] = {
                    'target_value' : new_id,
                    'updated' : datetime.now()
                }


        return new_id
