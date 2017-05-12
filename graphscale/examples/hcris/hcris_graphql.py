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

from .generated.hcris_graphql_generated import (
    GraphQLProvider,
    GraphQLProviderCsvRow,
    GraphQLReport,
)

from graphscale.utils import print_error

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

def create_browse_field(graphql_type, pent_type):
    async def browse_resolver(_parent, args, context, *_):
        try:
            after = None
            if 'after' in args:
                after = UUID(hex=args['after'])
            first = args.get('first', 1000) # cap at 1000 to prevent timeouts for now
            return await pent_type.gen_all(context, after, first)
        except Exception as error:
            print_error(error)

    return GraphQLField(
        type=req(list_of(req(graphql_type))),
        args={
            'after': GraphQLArgument(type=GraphQLID),
            'first': GraphQLArgument(type=GraphQLInt),
        },
        resolver=browse_resolver,
    )

class HcrisSchema:
    @staticmethod
    def graphql_schema():
        return GraphQLSchema(
            query=GraphQLObjectType(
                name='Query',
                fields=lambda: {**generated_query_fields(pent_map()), **{
                    # custom fields go here
                    'allProviders': create_browse_field(GraphQLProvider.type(), Provider),
                    'allReports': create_browse_field(GraphQLReport.type(), Report),
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

def create_hcris_schema():
    return HcrisSchema.graphql_schema()

async def create_hospital_resolver(_parent, args, context, *_):
    hospital_input = ProviderCsvRow(args['input'])
    return await create_provider(context, hospital_input)
