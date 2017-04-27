# from graphscale.kvetch.kvetch_db import (
#     kv_gen_object,
#     kv_gen_objects
# )

# class PentConfig:

#     _id_to_cls = None
#     _cls_to_id = None

#     @staticmethod
#     def _get_id_to_cls_map():
#         if PentConfig._id_to_cls is not None:
#             return PentConfig._id_to_cls

#         PentConfig._id_to_cls = {
#             1000: TodoUser,
#             1001: TodoItem,
#         }
#         return PentConfig._id_to_cls

#     @staticmethod
#     def _get_cls_to_id_map():
#         if PentConfig._cls_to_id is not None:
#             return PentConfig._cls_to_id

#         # swap keys and values
#         PentConfig._cls_to_id = {v:k for k, v in PentConfig._get_id_to_cls_map().items()}
#         return PentConfig._cls_to_id

#     @staticmethod
#     def get_type(type_id):
#         return PentConfig._get_id_to_cls_map()[type_id]

#     @staticmethod
#     def get_type_id(klass):
#         return PentConfig._get_cls_to_id_map()[klass]


# async def _load_pent(context, id_):
#     data = await kv_gen_object(context, id_)
#     cls = PentConfig.get_type(data['__type_id'])
#     return cls(context, id_, data)

# async def _load_pents(context, ids):
#     obj_dict = await kv_gen_objects(context, ids)
#     pent_dict = {}
#     for id_, data in obj_dict.items():
#         cls = PentConfig.get_type(data['__type_id'])
#         pent_dict[id_] = cls(context, id_, data)
#     return pent_dict

# class Pent:
#     def __init__(self, context, id_, data):
#         self._context = context
#         self._id = id_
#         self._data = data

#     @staticmethod
#     async def genany(context, id_):
#         return await _load_pent(context, id_)

#     @classmethod
#     async def gen(cls, context, id_):
#         if cls == Pent:
#             return await Pent.genany(context, id_)

#         data = await kv_gen_object(context, id_)
#         return cls(context, id_, data)

#     @classmethod
#     async def gen_list(cls, context, ids):
#         if cls == Pent:
#             return await _load_pents(context, ids)
#         data_list = await kv_gen_objects(context, ids)
#         return [cls(context, data['id'], data) for data in data_list.values()]


#     def id_(self):
#         return self._id

#     def context(self):
#         return self._context

#     def data(self):
#         return self._data
