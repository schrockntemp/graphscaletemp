from graphql import(
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    GraphQLInputObjectType,
    GraphQLInputObjectField,
    GraphQLString,
    GraphQLArgument,
    # GraphQLID,
)

from graphscale.grapple import GrappleType, req

from .generated.hcris_graphql_generated import GraphQLHospital, GraphQLCreateHospitalInput
from .generated.hcris_graphql_generated import generated_query_fields

from .hcris_pent import Hospital, CreateHospitalInput, create_hospital

def pent_map():
    return {
        'Hospital': Hospital,
    }

def define_create_mutation(out_type, in_type, resolver):
    return GraphQLField(
        type=out_type.type(),
        args={
            'input': GraphQLArgument(type=req(in_type.type())),
        },
        resolver=resolver,
    )

def create_hcris_schema():
    return GraphQLSchema(
        query=GraphQLObjectType(
            name='Query',
            fields=lambda: {**generated_query_fields(pent_map()), **{
                # custom fields go here
            }},
        ),
        mutation=GraphQLObjectType(
            name='Mutation',
            fields=lambda: {
                'createHospital' : define_create_mutation(
                    GraphQLHospital,
                    GraphQLCreateHospitalInput,
                    create_hospital_resolver),
            },
        ),
    )

async def create_hospital_resolver(_parent, args, context, *_):
    hospital_input = CreateHospitalInput(provider=args['input']['provider'])
    return await create_hospital(context, hospital_input)
