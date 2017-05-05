from uuid import UUID

from graphql import (
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    GraphQLString,
    GraphQLArgument,
)

from examples.todo.todo_pents import TodoUser

def get_pent_genner(klass):
    async def genner(_parent, args, context, *_):
        id_ = UUID(args['id'])
        return await klass.gen(context, id_)
    return genner

def define_top_level_getter(graphql_type, pent_type):
    return GraphQLField(
        type=graphql_type,
        args={'id': GraphQLArgument(type=GraphQLString)},
        resolver=get_pent_genner(pent_type)
    )

class GraphQLTodoUser:
    _memo = None
    @staticmethod
    def type():
        if GraphQLTodoUser._memo is not None:
            return GraphQLTodoUser._memo

        GraphQLTodoUser._memo = GraphQLObjectType(
            name='TodoUser',
            fields={
                'name': GraphQLField(
                    type=GraphQLString,
                    resolver=lambda user, *_: user.name(),
                ),
            },
        )
        return GraphQLTodoUser._memo

def create_todo_schema():
    return GraphQLSchema(
        query=GraphQLObjectType(
            name='Query',
            fields={
                'user': define_top_level_getter(GraphQLTodoUser.type(), TodoUser),
                #'user': GraphQLField(
                #    type=todo_user_type(),
                #    args={
                #        'id': GraphQLArgument(type=GraphQLString)
                #    },
                #    resolver=get_pent_genner(TodoUser),
                #),
            },
        ),
    )
