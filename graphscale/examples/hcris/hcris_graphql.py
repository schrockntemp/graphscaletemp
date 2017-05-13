from uuid import UUID

from graphql import(
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    # GraphQLInputObjectType,
    # GraphQLInputObjectField,
    # GraphQLString,
    GraphQLArgument,
    GraphQLID,
    GraphQLInt,
)

from graphscale.grapple import req, list_of
from graphscale.utils import print_error

from .generated.hcris_graphql_generated import (
    GraphQLProvider,
    GraphQLProviderCsvRow,
    GraphQLReport,
    GraphQLReportCsvRow,
)

from .generated.hcris_graphql_generated import generated_query_fields 

from .hcris_pent import (
    Provider,
    Report,
    ProviderCsvRow,
    create_provider,
    create_report,
    ReportCsvRow,
    WorksheetEntry,
    WorksheetInstance,
)

def define_create(out_type, in_type, resolver):
    return GraphQLField(
        type=out_type.type(),
        args={
            'input': GraphQLArgument(type=req(in_type.type())),
        },
        resolver=resolver,
    )

def pent_map():
    return {
        'Provider': Provider,
        'Report': Report,
        'WorksheetInstance': WorksheetInstance,
        'WorksheetEntry': WorksheetEntry,
    }

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
        report_type = GraphQLProvider.type()
        report_type.fields['reports'].resolver = reports_resolver

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
                        create_provider_resolver),
                    'createReport' : define_create(
                        GraphQLReport,
                        GraphQLReportCsvRow,
                        create_report_resolver),
                },
            ),
        )

def create_hcris_schema():
    return HcrisSchema.graphql_schema()

async def reports_resolver(provider, args, *_):
    try:
        return await provider.gen_reports(after=args.get('after'), first=args.get('first'))
    except Exception as e:
        import traceback
        traceback.print_exc()
        print_error(e)
        return []

async def create_provider_resolver(_parent, args, context, *_):
    ## create
    hospital_input = ProviderCsvRow(args['input'])
    return await create_provider(context, hospital_input)

async def create_report_resolver(_parent, args, context, *_):
    ## create
    report_input = ReportCsvRow(args['input'])
    return await create_report(context, report_input)
