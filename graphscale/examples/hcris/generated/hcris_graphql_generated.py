#W0661: unused imports lint
#C0301: line too long
#pylint: disable=W0611,C0301

from graphql import (
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    GraphQLString,
    GraphQLArgument,
    GraphQLList,
    GraphQLInt,
    GraphQLInputObjectType,
    GraphQLInputObjectField,
    GraphQLNonNull,
    GraphQLID,
    GraphQLEnumType,
)

from graphql.type import GraphQLEnumValue

from graphscale.grapple import (
    GrappleType,
    id_field,
    req,
    list_of,
    define_top_level_getter,
    GraphQLDate,
)

class GraphQLProvider(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Provider',
            fields=lambda: {
                'id': GraphQLField(
                    type=req(GraphQLID),
                    resolver=lambda obj, args, *_: obj.obj_id(*args),
                ),
                'name': GraphQLField(type=req(GraphQLString)),
                'providerNumber': GraphQLField(
                    type=req(GraphQLInt),
                    resolver=lambda obj, args, *_: obj.provider_number(*args),
                ),
                'fiscalYearBegin': GraphQLField(
                    type=req(GraphQLDate.type()),
                    resolver=lambda obj, args, *_: obj.fiscal_year_begin(*args),
                ),
                'fiscalYearEnd': GraphQLField(
                    type=req(GraphQLDate.type()),
                    resolver=lambda obj, args, *_: obj.fiscal_year_end(*args),
                ),
                'status': GraphQLField(
                    type=req(GraphQLProviderStatus.type()),
                    resolver=lambda obj, args, *_: obj.status(*args).name if obj.status(*args) else None,
                ),
                'streetAddress': GraphQLField(
                    type=req(GraphQLString),
                    resolver=lambda obj, args, *_: obj.street_address(*args),
                ),
                'poBox': GraphQLField(
                    type=req(GraphQLString),
                    resolver=lambda obj, args, *_: obj.po_box(*args),
                ),
                'city': GraphQLField(type=req(GraphQLString)),
                'state': GraphQLField(type=req(GraphQLString)),
                'zipCode': GraphQLField(
                    type=req(GraphQLString),
                    resolver=lambda obj, args, *_: obj.zip_code(*args),
                ),
                'county': GraphQLField(type=req(GraphQLString)),
                'reports': GraphQLField(
                    type=req(list_of(req(GraphQLReport.type()))),
                    args={
                        'after': GraphQLArgument(type=GraphQLID),
                        'first': GraphQLArgument(type=GraphQLInt),
                    },
                ),
            },
        )

class GraphQLReport(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Report',
            fields=lambda: {
                'id': GraphQLField(
                    type=req(GraphQLID),
                    resolver=lambda obj, args, *_: obj.obj_id(*args),
                ),
                'provider': GraphQLField(type=GraphQLProvider.type()),
                'reportRecordNumber': GraphQLField(
                    type=req(GraphQLInt),
                    resolver=lambda obj, args, *_: obj.report_record_number(*args),
                ),
                'providerNumber': GraphQLField(
                    type=req(GraphQLInt),
                    resolver=lambda obj, args, *_: obj.provider_number(*args),
                ),
                'fiscalIntermediaryNumber': GraphQLField(
                    type=req(GraphQLInt),
                    resolver=lambda obj, args, *_: obj.fiscal_intermediary_number(*args),
                ),
                'processDate': GraphQLField(
                    type=req(GraphQLDate.type()),
                    resolver=lambda obj, args, *_: obj.process_date(*args),
                ),
                'medicareUtilizationLevel': GraphQLField(
                    type=req(GraphQLMedicareUtilizationLevel.type()),
                    resolver=lambda obj, args, *_: obj.medicare_utilization_level(*args).name if obj.medicare_utilization_level(*args) else None,
                ),
                'worksheetInstances': GraphQLField(
                    type=req(list_of(req(GraphQLWorksheetInstance.type()))),
                    args={
                        'after': GraphQLArgument(type=GraphQLID),
                        'first': GraphQLArgument(type=GraphQLInt),
                    },
                    resolver=lambda obj, args, *_: obj.worksheet_instances(*args),
                ),
            },
        )

class GraphQLWorksheetInstance(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='WorksheetInstance',
            fields=lambda: {
                'id': GraphQLField(
                    type=req(GraphQLID),
                    resolver=lambda obj, args, *_: obj.obj_id(*args),
                ),
                'reportRecordNumber': GraphQLField(
                    type=req(GraphQLInt),
                    resolver=lambda obj, args, *_: obj.report_record_number(*args),
                ),
                'worksheetCode': GraphQLField(
                    type=req(GraphQLString),
                    resolver=lambda obj, args, *_: obj.worksheet_code(*args),
                ),
                'entries': GraphQLField(type=req(list_of(req(GraphQLWorksheetEntry.type())))),
            },
        )

class GraphQLWorksheetEntry(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='WorksheetEntry',
            fields=lambda: {
                'line': GraphQLField(type=req(GraphQLString)),
                'subline': GraphQLField(type=GraphQLString),
                'column': GraphQLField(type=req(GraphQLString)),
                'subcolumn': GraphQLField(type=GraphQLString),
                'value': GraphQLField(type=GraphQLString),
                'valueAsInt': GraphQLField(
                    type=GraphQLInt,
                    resolver=lambda obj, args, *_: obj.value_as_int(*args),
                ),
            },
        )

class GraphQLProviderCsvRow(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLInputObjectType(
            name='ProviderCsvRow',
            fields=lambda: {
                'provider': GraphQLInputObjectField(type=req(GraphQLString)),
                'fyb': GraphQLInputObjectField(type=req(GraphQLString)),
                'fye': GraphQLInputObjectField(type=req(GraphQLString)),
                'status': GraphQLInputObjectField(type=req(GraphQLString)),
                'ctrl_type': GraphQLInputObjectField(type=req(GraphQLString)),
                'hosp_name': GraphQLInputObjectField(type=req(GraphQLString)),
                'street_addr': GraphQLInputObjectField(type=GraphQLString),
                'po_box': GraphQLInputObjectField(type=GraphQLString),
                'city': GraphQLInputObjectField(type=req(GraphQLString)),
                'state': GraphQLInputObjectField(type=req(GraphQLString)),
                'zip_code': GraphQLInputObjectField(type=req(GraphQLString)),
                'county': GraphQLInputObjectField(type=GraphQLString),
            },
        )

class GraphQLReportCsvRow(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLInputObjectType(
            name='ReportCsvRow',
            fields=lambda: {
                'rpt_rec_num': GraphQLInputObjectField(type=req(GraphQLString)),
                'prvdr_ctrl_type_cd': GraphQLInputObjectField(type=req(GraphQLString)),
                'prvdr_num': GraphQLInputObjectField(type=req(GraphQLString)),
                'rpt_stus_cd': GraphQLInputObjectField(type=req(GraphQLString)),
                'initl_rpt_sw': GraphQLInputObjectField(type=req(GraphQLString)),
                'last_rpt_sw': GraphQLInputObjectField(type=req(GraphQLString)),
                'trnsmtl_num': GraphQLInputObjectField(type=req(GraphQLString)),
                'fi_num': GraphQLInputObjectField(type=req(GraphQLString)),
                'adr_vndr_cd': GraphQLInputObjectField(type=req(GraphQLString)),
                'util_cd': GraphQLInputObjectField(type=req(GraphQLString)),
                'spec_ind': GraphQLInputObjectField(type=GraphQLString),
                'npi': GraphQLInputObjectField(type=GraphQLString),
                'fy_bgn_dt': GraphQLInputObjectField(type=req(GraphQLString)),
                'fy_end_dt': GraphQLInputObjectField(type=req(GraphQLString)),
                'proc_dt': GraphQLInputObjectField(type=req(GraphQLString)),
                'fi_creat_dt': GraphQLInputObjectField(type=req(GraphQLString)),
                'npr_dt': GraphQLInputObjectField(type=GraphQLString),
                'fi_rcpt_dt': GraphQLInputObjectField(type=req(GraphQLString)),
            },
        )

class GraphQLWorksheetEntryInput(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLInputObjectType(
            name='WorksheetEntryInput',
            fields=lambda: {
                'line': GraphQLInputObjectField(type=req(GraphQLString)),
                'column': GraphQLInputObjectField(type=req(GraphQLString)),
                'value': GraphQLInputObjectField(type=req(GraphQLString)),
            },
        )

class GraphQLCreateWorksheetInstanceInput(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLInputObjectType(
            name='CreateWorksheetInstanceInput',
            fields=lambda: {
                'reportRecordNumber': GraphQLInputObjectField(type=req(GraphQLInt)),
                'worksheetCode': GraphQLInputObjectField(type=req(GraphQLString)),
                'worksheetEntries': GraphQLInputObjectField(type=req(list_of(req(GraphQLWorksheetEntryInput.type())))),
            },
        )

class GraphQLProviderStatus(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLEnumType(
            name='ProviderStatus',
            values={
                'AS_SUBMITTED': GraphQLEnumValue(),
                'SETTLED': GraphQLEnumValue(),
                'AMENDED': GraphQLEnumValue(),
                'SETTLED_WITH_AUDIT': GraphQLEnumValue(),
                'REOPENED': GraphQLEnumValue(),
            },
        )

class GraphQLMedicareUtilizationLevel(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLEnumType(
            name='MedicareUtilizationLevel',
            values={
                'NONE': GraphQLEnumValue(),
                'LOW': GraphQLEnumValue(),
                'FULL': GraphQLEnumValue(),
            },
        )

def generated_query_fields(pent_map):
    return {
        'provider': define_top_level_getter(GraphQLProvider.type(), pent_map['Provider']),
        'report': define_top_level_getter(GraphQLReport.type(), pent_map['Report']),
        'worksheetInstance': define_top_level_getter(GraphQLWorksheetInstance.type(), pent_map['WorksheetInstance']),
    }

