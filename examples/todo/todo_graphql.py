from graphql import (
    graphql,
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    GraphQLString,
    GraphQLArgument,
)

from examples.todo.todo_pents import TodoUser

from uuid import UUID

#def resolve_return_arg(source, args, context, info, executor, ex):
async def resolve_return_arg(parent_object, args, context, ok, *argz): # info, executor, ex):
    # print('ARGS')
    # print(args)
    # print('context')
    # print(context)
    # print('ok')
    # print(ok)
    return args['value']

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

# async def gen_user(parent, args, context, *argz):
#     id_ = UUID(args['id'])
#     return await TodoUser.gen(context, id_)

def todo_user_type():
    return GraphQLObjectType(
        name='TodoUser',
        fields={
            'name': GraphQLField(
                type=GraphQLString,
                resolver=lambda user, *_: user.name(),
            ),
        },
    )

def create_todo_schema():
    return GraphQLSchema(
        query=GraphQLObjectType(
            name='Query',
            fields={
                'hello': GraphQLField(
                    type=GraphQLString,
                    resolver=lambda *_: 'world',
                ),
                'return_arg': GraphQLField(
                    type=GraphQLString,
                    args={
                        'value': GraphQLArgument(type=GraphQLString),
                    },
                    resolver=resolve_return_arg
                ),
                'user': define_top_level_getter(todo_user_type(), TodoUser),
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
