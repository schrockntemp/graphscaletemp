from uuid import UUID

from graphql import (
    GraphQLSchema,
    GraphQLObjectType,
    GraphQLField,
    GraphQLString,
    GraphQLArgument,
    GraphQLList,
)

from examples.todo.todo_pents import TodoUser, TodoItem

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

async def gen_todo_items(user, args, _context, *_):
    return await user.gen_todo_items(after=args.get('after'), first=args.get('first'))

class GraphQLTodoUser:
    _memo = None
    @staticmethod
    def type():
        if GraphQLTodoUser._memo is not None:
            return GraphQLTodoUser._memo

        GraphQLTodoUser._memo = GraphQLObjectType(
            name='TodoUser',
            fields={
                'name': GraphQLField(type=GraphQLString),
                'todoItems': GraphQLField(
                    type=GraphQLList(GraphQLTodoItem.type()),
                    resolver=gen_todo_items
                ),
            },
        )
        return GraphQLTodoUser._memo

class GraphQLTodoItem:
    _memo = None
    @staticmethod
    def type():
        if GraphQLTodoItem._memo is not None:
            return GraphQLTodoItem._memo

        GraphQLTodoItem._memo = GraphQLObjectType(
            name='TodoItem',
            fields={
                'text': GraphQLField(type=GraphQLString),
            },
        )
        return GraphQLTodoItem._memo

def create_todo_schema():
    return GraphQLSchema(
        query=GraphQLObjectType(
            name='Query',
            fields={
                'user': define_top_level_getter(GraphQLTodoUser.type(), TodoUser),
                'todoItem': define_top_level_getter(GraphQLTodoItem.type(), TodoItem),
            },
        ),
    )
