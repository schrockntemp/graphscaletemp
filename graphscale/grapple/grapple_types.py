from uuid import UUID

import datetime
import iso8601

from graphql import (
    GraphQLField,
    GraphQLNonNull,
    GraphQLID,
    GraphQLList,
    GraphQLArgument,
    GraphQLObjectType,
    GraphQLScalarType,
)

from graphql.language.ast import StringValue

from graphscale.utils import param_check

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

def define_top_level_getter(graphql_type, pent_type):
    param_check(graphql_type, GraphQLObjectType, 'graphql_type')
    return GraphQLField(
        type=graphql_type,
        args={'id': GraphQLArgument(type=GraphQLID)},
        resolver=get_pent_genner(pent_type)
    )

def get_pent_genner(klass):
    async def genner(_parent, args, context, *_):
        obj_id = UUID(args['id'])
        return await klass.gen(context, obj_id)
    return genner

def serialize_date(date):
    return date.isoformat()

def coerce_date(value):
    if isinstance(value, datetime.date):
        return value
    if isinstance(value, datetime.datetime):
        return datetime.date(value.year, value.month. value.day)
    if isinstance(value, str):
        return iso8601.parse_date(value) # let's see what we can do
    return None # should I throw?

def parse_date_literal(ast):
    if isinstance(ast, StringValue):
        return iso8601.parse_date(ast.value)

class GraphQLDate(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLScalarType(
            name='Date',
            description='ISO-8601 Date',
            serialize=serialize_date,
            parse_value=coerce_date,
            parse_literal=parse_date_literal
    )
