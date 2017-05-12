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

from .generated.hcris_graphql_generated import GraphQLProvider, GraphQLProviderCsvRow 
from .generated.hcris_graphql_generated import generated_query_fields

from .hcris_pent import Provider, Report, ProviderCsvRow, create_provider

def pent_map():
    return {
        'Provider': Provider,
        'Report': Report,
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
                'allProviders': GraphQLField(
                    type=req(list_of(req(GraphQLProvider.type()))),
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
                'createProvider' : define_create(
                    GraphQLProvider,
                    GraphQLProviderCsvRow,
                    create_hospital_resolver),
            },
        ),
    )

async def create_hospital_resolver(_parent, args, context, *_):
    hospital_input = ProviderCsvRow(args['input'])
    return await create_provider(context, hospital_input)

async def all_hospitals_resolver(_parent, args, context, *_):
    try:
        after = None
        if args.get('after'):
            after = UUID(hex=args['after'])
        first = args.get('first')
        return await Provider.gen_all(context, after, first)
    except Exception as error:
        import sys
        sys.stderr.write(error)
