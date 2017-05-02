from datetime import datetime

from graphql.language.source import Source
from graphql.language.parser import parse
from graphql.language.ast import ObjectTypeDefinition

def parse_grapple(grapple_string):
    ast = parse(Source(grapple_string))
    grapple_object_types = [create_grapple_type(type_node) for type_node in ast.definitions]
    return GrappleDocument(object_types=grapple_object_types)

def print_grapple(document_ast):
    writer = CodeWriter()
    for object_type in document_ast.object_types():
        print_generated_pent(writer, object_type)
    writer.blank_line()
    return writer.result()

def graphql_type_to_python_type(graphql_type):
    scalars = {
        'Int' : int,
        'Float' : float,
        'String' : str,
        'Boolean' : bool,
        'ID' : str,
        'DateTime' : datetime,
    }
    if graphql_type in scalars:
        return scalars[graphql_type].__name__
    return graphql_type

class GrappleDocument:
    def __init__(self, *, object_types):
        self._object_types = object_types

    def object_types(self):
        return self._object_types

class GrappleType:
    def __init__(self, *, name, fields):
        self._name = name
        self._fields = fields

    def name(self):
        return self._name

    def fields(self):
        return self._fields

class GrappleField:
    def __init__(self, *, name, graphql_type, python_type):
        self._name = name
        self._graphql_type = graphql_type
        self._python_type = python_type

    def name(self):
        return self._name

    def graphql_type(self):
        return self._graphql_type

    def python_type(self):
        return self._python_type


def filter_nodes(nodes, ast_cls):
    return filter(lambda node: isinstance(node, ast_cls), nodes)

def has_generate_pent_directive(type_ast):
    return 'generatePent' in [dir_node.name.value for dir_node in type_ast.directives]

def create_grapple_type(type_ast):
    if isinstance(type_ast, ObjectTypeDefinition):
        if has_generate_pent_directive(type_ast):
            return create_grapple_object_type(type_ast)
    else:
        raise Exception('node not supported: ' + str(type_ast))

def create_grapple_object_type(object_type_ast):
    grapple_type_name = object_type_ast.name.value
    grapple_fields = [create_grapple_field(field) for field in object_type_ast.fields]
    return GrappleType(name=grapple_type_name, fields=grapple_fields)

def create_grapple_field(graphql_field):
    graphql_type = graphql_field.type.name.value
    return GrappleField(
        name=graphql_field.name.value,
        # only named type for now
        graphql_type=graphql_type,
        python_type=graphql_type_to_python_type(graphql_type),
    )

class CodeWriter:
    def __init__(self):
        self.lines = []
        self.indent = 0

    def line(self, text):
        self.lines.append((" " * self.indent) + text)

    def blank_line(self):
        self.lines.append("")

    def increase_indent(self):
        self.indent += 4

    def decrease_indent(self):
        if self.indent <= 0:
            raise Exception('indent cannot be negative')
        self.indent -= 4

    def result(self):
        return "\n".join(self.lines)


def print_generated_pent(writer, grapple_type):
    writer.line('class %sGenerated(Pent):' % grapple_type.name())
    writer.increase_indent() # begin class implementation
    writer.blank_line()
    print_is_db_data_valid(writer, grapple_type)
    print_generated_fields(writer, grapple_type.fields())
    writer.decrease_indent() # end class definition

def print_if_return_false(writer, if_line):
    writer.line(if_line)
    writer.increase_indent()
    writer.line('return False')
    writer.decrease_indent()

def print_is_db_data_valid(writer, grapple_type):
    writer.line('@staticmethod')
    writer.line('# This method checks to see that data coming out of the database is valid')
    writer.line('def is_db_data_valid(data):')
    writer.increase_indent() # begin is_db_data_valid implementation
    print_if_return_false(writer, 'if not isinstance(data, dict):')
    print_if_return_false(writer, "if req_data_elem_invalid(data, 'id', UUID): # id: ID!")
    for field in grapple_type.fields():
        if_line = "if opt_data_elem_invalid(data, '%s', %s):" % (field.name(), field.python_type())
        if_line += ' # %s: %s' % (field.name(), field.graphql_type())
        print_if_return_false(writer, if_line)
    writer.line('return True')
    writer.decrease_indent() # end is_db_data_valid implementation
    writer.blank_line()

def print_generated_fields(writer, fields):
    for field in fields:
        writer.line('def %s(self):' % field.name())
        writer.increase_indent() # begin property implemenation
        writer.line("return self._data['%s']" % field.name())
        writer.decrease_indent() # end property definition

