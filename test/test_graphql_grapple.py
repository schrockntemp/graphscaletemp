from graphscale.grapple.grapple_parser import parse_grapple, print_graphql_defs

def test_basic_type():
    graphql = """type Test { name: String }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields={
                'name': GraphQLField(type=GraphQLString),
            },
        )
"""

def test_nonnullable_type():
    graphql = """type Test { name: String! }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields={
                'name': GraphQLField(type=req(GraphQLString)),
            },
        )
"""

def test_list_type():
    graphql = """type Test { names: [String] }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields={
                'names': GraphQLField(type=list_of(GraphQLString)),
            },
        )
"""

def test_list_of_reqs():
    graphql = """type Test { names: [String!] }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields={
                'names': GraphQLField(type=list_of(req(GraphQLString))),
            },
        )
"""
def test_req_list():
    graphql = """type Test { names: [String]! }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields={
                'names': GraphQLField(type=req(list_of(GraphQLString))),
            },
        )
"""

def test_req_list_of_reqs():
    graphql = """type Test { names: [String!]! }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields={
                'names': GraphQLField(type=req(list_of(req(GraphQLString)))),
            },
        )
"""

def test_double_list():
    graphql = """type Test { nameMatrix: [[String]] }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields={
                'nameMatrix': GraphQLField(type=list_of(list_of(GraphQLString))),
            },
        )
"""