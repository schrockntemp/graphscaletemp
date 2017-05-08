from graphscale.grapple.grapple_parser import (
    parse_grapple,
    print_grapple_pents,
    graphql_type_to_python_type,
)

def test_no_grapple_types():
    grapple_string = """type TestObjectField {bar: FooBar}"""
    grapple_document = parse_grapple(grapple_string)
    output = print_grapple_pents(grapple_document)
    assert output == ""

def test_ignore_type():
    grapple_string = """type TestObjectField @generatePent {bar: FooBar} type Other { }"""
    grapple_document = parse_grapple(grapple_string)
    output = print_grapple_pents(grapple_document)
    assert output == \
"""class TestObjectFieldGenerated(Pent):

    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(data):
        if not isinstance(data, dict):
            return False
        if opt_data_elem_invalid(data, 'bar', FooBar): # bar: FooBar
            return False
        return True

    def bar(self):
        return self._data.get('bar')

"""

def test_required_object_field():
    grapple_string = """type TestObjectField @generatePent {bar: FooBar!}"""
    grapple_document = parse_grapple(grapple_string)
    output = print_grapple_pents(grapple_document)
    assert output == \
"""class TestObjectFieldGenerated(Pent):

    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(data):
        if not isinstance(data, dict):
            return False
        if req_data_elem_invalid(data, 'bar', FooBar): # bar: FooBar!
            return False
        return True

    def bar(self):
        return self._data['bar']

"""

def test_object_field():
    grapple_string = """type TestObjectField @generatePent {bar: FooBar}"""
    grapple_document = parse_grapple(grapple_string)
    output = print_grapple_pents(grapple_document)
    assert output == \
"""class TestObjectFieldGenerated(Pent):

    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(data):
        if not isinstance(data, dict):
            return False
        if opt_data_elem_invalid(data, 'bar', FooBar): # bar: FooBar
            return False
        return True

    def bar(self):
        return self._data.get('bar')

"""

def test_required_field():
    grapple_string = """type TestRequired @generatePent {id: ID!, name: String!}"""
    grapple_document = parse_grapple(grapple_string)
    output = print_grapple_pents(grapple_document)
    # print('OUTPUT')
    # print(output.replace(' ', '-'))
    # print('END OUTPUT')
    assert output == \
"""class TestRequiredGenerated(Pent):

    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(data):
        if not isinstance(data, dict):
            return False
        if req_data_elem_invalid(data, 'id', UUID): # id: ID!
            return False
        if req_data_elem_invalid(data, 'name', str): # name: String!
            return False
        return True

    def id_(self):
        return self._data['id']

    def name(self):
        return self._data['name']

"""

def test_single_nullable_field():
    grapple_string = """type Test @generatePent {name: String}"""
    grapple_document = parse_grapple(grapple_string)
    grapple_type = grapple_document.object_types()[0]
    assert grapple_type.name() == 'Test'
    fields = grapple_type.fields()
    assert len(fields) == 1
    name_field = fields[0]
    assert name_field.name() == 'name'
    assert name_field.type_ref().graphql_type() == 'String'
    assert name_field.type_ref().python_type() == 'str'
    output = print_grapple_pents(grapple_document)
    assert output == \
"""class TestGenerated(Pent):

    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(data):
        if not isinstance(data, dict):
            return False
        if opt_data_elem_invalid(data, 'name', str): # name: String
            return False
        return True

    def name(self):
        return self._data.get('name')

"""

def test_graphql_type_conversion():
    assert graphql_type_to_python_type('String') == 'str'
    assert graphql_type_to_python_type('Int') == 'int'
    assert graphql_type_to_python_type('SomeType') == 'SomeType'
