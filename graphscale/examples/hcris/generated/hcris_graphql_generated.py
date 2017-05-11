#W0661: unused imports lint
#pylint: disable=W0611

from graphql import (
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    GraphQLString,
    GraphQLArgument,
    GraphQLList,
    GraphQLInt,
    GraphQLInputObjectField,
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
                    resolver=lambda obj, args, *_: obj.status().name if obj else None,
                ),
            },
        )

class GraphQLHospitalStatus(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLEnumType(
            name='HospitalStatus',
            values={
                'AS_SUBMITTED': GraphQLEnumValue()
            }
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
            },
        )

def generated_query_fields(pent_map):
    return {
        'hospital': define_top_level_getter(GraphQLHospital.type(), pent_map['Hospital']),
    }

