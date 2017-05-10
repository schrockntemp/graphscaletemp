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
    GraphQLInputObjectType,
    GraphQLInputObjectField,
    GraphQLNonNull,
    GraphQLID,
)

from graphscale.grapple import (
    GrappleType,
    id_field,
    req,
    list_of
)

class GraphQLHospital(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Hospital',
            fields=lambda: {
                'providerNumber': GraphQLField(
                    type=req(GraphQLString),
                    resolver=lambda obj, args, *_: obj.provider_number(*args),
                ),
            },
        )
