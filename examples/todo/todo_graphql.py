from uuid import UUID

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

from examples.todo.todo_pents import (
    TodoUser,
    TodoUserInput,
    TodoItem,
    create_todo_user,
    update_todo_user,
    delete_todo_user,
)

class GrappleType:
    _memo = {}
    @classmethod
    def type(cls):
        if cls in GrappleType._memo:
            return GrappleType._memo[cls]
        new_type = cls.create_type()
        GrappleType._memo[cls] = new_type
        return new_type

    @classmethod
    def create_type(cls):
        raise Exception('must implement @classmethod create_type')

def get_pent_genner(klass):
    async def genner(_parent, args, context, *_):
        obj_id = UUID(args['id'])
        return await klass.gen(context, obj_id)
    return genner

def define_top_level_getter(graphql_type, pent_type):
    return GraphQLField(
        type=graphql_type,
        args={'id': GraphQLArgument(type=GraphQLString)},
        resolver=get_pent_genner(pent_type)
    )

async def gen_todo_items(user, args, _context, *_):
    return await user.gen_todo_items(after=args.get('after'), first=args.get('first'))

def id_field():
    return GraphQLField(
        type=GraphQLNonNull(type=GraphQLID),
        resolver=lambda obj, *_: obj.obj_id(),
    )

def req(ttype):
    return GraphQLNonNull(type=ttype)

def list_of(ttype):
    return GraphQLList(type=ttype)

class GraphQLTodoUser(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='TodoUser',
            fields=lambda: {
                'id': id_field(),
                'name': GraphQLField(type=GraphQLString),
                'capitalizedName': GraphQLField(
                    type=GraphQLString,
                    resolver=lambda obj, args, *_: obj.capitalized_name(*args),
                ),
                'todoItems': GraphQLField(
                    type=req(list_of(req(GraphQLTodoItem.type()))),
                    args={
                        'first': GraphQLArgument(type=GraphQLInt),
                        'after': GraphQLArgument(type=GraphQLString),
                    },
                    resolver=gen_todo_items
                ),
            },
        )

class GraphQLTodoItem(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='TodoItem',
            fields=lambda: {
                'text': GraphQLField(type=GraphQLString),
            },
        )

class GraphQLTodoUserInput(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLInputObjectType(
            name='TodoUserInput',
            fields=lambda: {
                'name': GraphQLInputObjectField(type=req(GraphQLString)),
            },
        )

def create_todo_schema():
    return GraphQLSchema(
        query=GraphQLObjectType(
            name='Query',
            fields=lambda: {
                'user': define_top_level_getter(GraphQLTodoUser.type(), TodoUser),
                'todoItem': define_top_level_getter(GraphQLTodoItem.type(), TodoItem),
            },
        ),
        mutation=GraphQLObjectType(
            name='Mutation',
            fields=lambda: {
                'createUser': GraphQLField(
                    type=GraphQLTodoUser.type(),
                    args={
                        'input': GraphQLArgument(type=req(GraphQLTodoUserInput.type())),
                    },
                    resolver=create_user_resolver
                ),
                'updateUser': GraphQLField(
                    type=GraphQLTodoUser.type(),
                    args={
                        'id': GraphQLArgument(type=req(GraphQLID)),
                        'input': GraphQLArgument(type=req(GraphQLTodoUserInput.type())),
                    },
                    resolver=update_user_resolver
                ),
                'deleteUser': GraphQLField(
                    type=GraphQLTodoUser.type(),
                    args={
                        'id': GraphQLArgument(type=req(GraphQLID)),
                    },
                    resolver=delete_user_resolver
                ),
            },
        ),
    )

async def create_user_resolver(_parent, args, context, *_):
    user_input = TodoUserInput(name=args['input']['name'])
    return await create_todo_user(context, user_input)

async def update_user_resolver(_parent, args, context, *_):
    obj_id = UUID(args['id'])
    name = args['input']['name']
    user_input = TodoUserInput(name=name)
    return await update_todo_user(context, obj_id, user_input)

async def delete_user_resolver(_parent, args, context, *_):
    obj_id = UUID(args['id'])
    old_user = await TodoUser.gen(context, obj_id)
    await delete_todo_user(context, obj_id)
    return old_user
