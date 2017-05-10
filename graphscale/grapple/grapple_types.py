from graphql import (
    GraphQLField,
    GraphQLNonNull,
    GraphQLID,
    GraphQLList,
)

class GrappleType:
    _memo = {}
    @classmethod
    def type(cls):
        if cls in GrappleType._memo:
            return GrappleType._memo[cls]
        new_type = cls.create_type()
        GrappleType._memo[cls] = new_type
        return new_type

    @classmethod
    def create_type(cls):
        raise Exception('must implement @classmethod create_type')

def id_field():
    return GraphQLField(
        type=GraphQLNonNull(type=GraphQLID),
        resolver=lambda obj, *_: obj.obj_id(),
    )

def req(ttype):
    return GraphQLNonNull(type=ttype)

def list_of(ttype):
    return GraphQLList(type=ttype)
