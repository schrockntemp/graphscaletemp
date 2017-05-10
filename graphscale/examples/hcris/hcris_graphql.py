from uuid import UUID

from graphql import(
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    GraphQLArgument,
    GraphQLID
)

from .generated.hcris_graphql_generated import GraphQLHospital
from .hcris_pent import Hospital

def define_top_level_getter(graphql_type, pent_type):
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

def create_hcris_schema():
    return GraphQLSchema(
        query=GraphQLObjectType(
            name='Query',
            fields=lambda: {
                'hospital': define_top_level_getter(GraphQLHospital.type(), Hospital),
            },
        ),
    )
