from uuid import UUID

from graphql import(
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    GraphQLInputObjectType,
    GraphQLInputObjectField,
    GraphQLString,
    GraphQLArgument,
    GraphQLID,
    GraphQLInt,
)

from graphscale.grapple import GrappleType, req, list_of

from .generated.hcris_graphql_generated import GraphQLHospital, GraphQLCreateHospitalInput
from .generated.hcris_graphql_generated import generated_query_fields

from .hcris_pent import Hospital, CreateHospitalInput, create_hospital

def pent_map():
    return {
        'Hospital': Hospital,
    }

def define_create(out_type, in_type, resolver):
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
                'allHospitals': GraphQLField(
                    type=req(list_of(req(GraphQLHospital.type()))),
                    args={
                        'after': GraphQLArgument(type=GraphQLID),
                        'first': GraphQLArgument(type=GraphQLInt),
                    },
                    resolver=all_hospitals_resolver,
                ),
                # custom fields go here
            }},
        ),
        mutation=GraphQLObjectType(
            name='Mutation',
            fields=lambda: {
                'createHospital' : define_create(
                    GraphQLHospital,
                    GraphQLCreateHospitalInput,
                    create_hospital_resolver),
            },
        ),
    )

async def create_hospital_resolver(_parent, args, context, *_):
    hospital_input = CreateHospitalInput(args['input'])
    return await create_hospital(context, hospital_input)

async def all_hospitals_resolver(_parent, args, context, *_):
    try:
        after = None
        if args.get('after'):
            after = UUID(hex=args['after'])
        first = args.get('first')
        return await Hospital.gen_all(context, after, first)
    except Exception as error:
        import sys
        sys.stderr.write(error)
