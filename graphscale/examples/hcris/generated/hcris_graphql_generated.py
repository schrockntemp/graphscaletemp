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

class GraphQLHospital(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Hospital',
            fields=lambda: {
                'id': GraphQLField(
                    type=req(GraphQLID),
                    resolver=lambda obj, args, *_: obj.obj_id(*args),
                ),
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
                    type=req(GraphQLHospitalStatus.type()),
                    resolver=lambda obj, args, *_: obj.status(*args).name if obj.status(*args) else None,
                ),
                'hospitalName': GraphQLField(
                    type=req(GraphQLString),
                    resolver=lambda obj, args, *_: obj.hospital_name(*args),
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
            },
        )

class GraphQLCreateHospitalInput(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLInputObjectType(
            name='CreateHospitalInput',
            fields=lambda: {
                'provider': GraphQLInputObjectField(type=req(GraphQLString)),
                'fyb': GraphQLInputObjectField(type=req(GraphQLString)),
                'fye': GraphQLInputObjectField(type=req(GraphQLString)),
                'status': GraphQLInputObjectField(type=req(GraphQLString)),
                'ctrl_type': GraphQLInputObjectField(type=req(GraphQLString)),
                'hosp_name': GraphQLInputObjectField(type=req(GraphQLString)),
                'street_addr': GraphQLInputObjectField(type=req(GraphQLString)),
                'po_box': GraphQLInputObjectField(type=GraphQLString),
                'city': GraphQLInputObjectField(type=req(GraphQLString)),
                'state': GraphQLInputObjectField(type=req(GraphQLString)),
                'zip_code': GraphQLInputObjectField(type=req(GraphQLString)),
                'county': GraphQLInputObjectField(type=req(GraphQLString)),
            },
        )

class GraphQLHospitalStatus(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLEnumType(
            name='HospitalStatus',
            values={
                'AS_SUBMITTED': GraphQLEnumValue(),
                'SETTLED': GraphQLEnumValue(),
                'AMENDED': GraphQLEnumValue(),
                'SETTLED_WITH_AUDIT': GraphQLEnumValue(),
                'REOPENED': GraphQLEnumValue(),
            },
        )

def generated_query_fields(pent_map):
    return {
        'hospital': define_top_level_getter(GraphQLHospital.type(), pent_map['Hospital']),
    }

