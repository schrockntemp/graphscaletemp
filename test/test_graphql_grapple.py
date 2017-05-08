import pytest

from graphscale.grapple.grapple_parser import parse_grapple, print_graphql_defs

def test_basic_type():
    graphql = """type Test { name: String }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields=lambda: {
                'name': GraphQLField(type=GraphQLString),
            },
        )
"""

@pytest.mark.skip
def test_non_pythonic_name():
    graphql = """type Test { longName: String }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields=lambda: {
                'longName': GraphQLField(
                    type=GraphQLString
                    resolver=lambda obj, args, *_: obj.long_name(*args)
                ),
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
            fields=lambda: {
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
            fields=lambda: {
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
            fields=lambda: {
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
            fields=lambda: {
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
            fields=lambda: {
                'names': GraphQLField(type=req(list_of(req(GraphQLString)))),
            },
        )
"""

def test_double_list():
    graphql = """type Test { matrix: [[String]] }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields=lambda: {
                'matrix': GraphQLField(type=list_of(list_of(GraphQLString))),
            },
        )
"""

def test_ref_to_self():
    graphql = """type Test { other: Test }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields=lambda: {
                'other': GraphQLField(type=GraphQLTest.type()),
            },
        )
"""

def test_args():
    graphql = """type Test { relatives(skip: Int, take: Int) : [Test] }"""
    result = print_graphql_defs(parse_grapple(graphql))
    assert result == """class GraphQLTest(GrappleType):
    @staticmethod
    def create_type():
        return GraphQLObjectType(
            name='Test',
            fields=lambda: {
                'relatives': GraphQLField(
                    type=list_of(GraphQLTest.type()),
                    args={
                        'skip': GraphQLArgument(type=GraphQLInt),
                        'take': GraphQLArgument(type=GraphQLInt),
                    },
                ),
            },
        )
"""