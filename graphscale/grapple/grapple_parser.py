import re

from datetime import datetime
from uuid import UUID
from enum import Enum, auto

from graphql.language.source import Source
from graphql.language.parser import parse
from graphql.language.ast import (
    ObjectTypeDefinition,
    NamedType,
    NonNullType,
    ListType,
    InputObjectTypeDefinition,
    EnumTypeDefinition,
)

from graphscale.utils import param_check


def parse_grapple(grapple_string):
    ast = parse(Source(grapple_string))
    grapple_types = []
    for type_node in ast.definitions:
        grapple_types.append(create_grapple_type_definition(type_node))
    return GrappleDocument(types=grapple_types)

def print_grapple_pents(document_ast):
    writer = CodeWriter()
    for object_type in document_ast.object_types():
        if object_type.generate_pent():
            print_generated_pent(writer, object_type)
    writer.blank_line()
    return writer.result()

def print_graphql_defs(document_ast):
    writer = CodeWriter()
    for object_type in document_ast.object_types():
        print_graphql_object_type(writer, document_ast, object_type)
    for input_type in document_ast.input_types():
        print_graphql_input_type(writer, input_type)
    for enum_type in document_ast.enum_types():
        print_graphql_enum_type(writer, enum_type)

    return writer.result()

def uncapitalized(string):
    return string[0].lower() + string[1:]

def print_graphql_top_level(document_ast):
    writer = CodeWriter()
    writer.line('def generated_query_fields(pent_map):')
    writer.increase_indent() # function body
    writer.line('return {')
    writer.increase_indent() # dictionary body
    for grapple_type in document_ast.object_types():
        if not grapple_type.has_field('id'): # needs to be fetchable
            continue
        field_name = uncapitalized(grapple_type.name())
        graphql_type_inst = 'GraphQL%s.type()' % grapple_type.name()
        writer.line("'%s': define_top_level_getter(%s, pent_map['%s'])," %
                    (field_name, graphql_type_inst, grapple_type.name()))
    writer.decrease_indent() # end dictionary body
    writer.line('}')
    writer.decrease_indent() # end function body
    writer.blank_line()
    return writer.result()

def print_graphql_input_type(writer, grapple_type):
    writer.line('class GraphQL%s(GrappleType):' % grapple_type.name())
    writer.increase_indent() # begin class definition
    writer.line('@staticmethod')
    writer.line('def create_type():')
    writer.increase_indent() # begin create_type body
    writer.line('return GraphQLInputObjectType(')
    writer.increase_indent() # begin GraphQLInputObjectType .ctor args
    writer.line("name='%s'," % grapple_type.name())
    writer.line('fields=lambda: {')
    writer.increase_indent() # begin field declarations
    for field in grapple_type.fields():
        print_graphql_input_field(writer, field)
    writer.decrease_indent() # end field declarations
    writer.line('},')
    writer.decrease_indent() # end GraphQLInputObjectType .ctor args
    writer.line(')')
    writer.decrease_indent() # end create_type body
    writer.decrease_indent() # end class definition
    writer.blank_line()

def print_graphql_enum_type(writer, grapple_type):
    writer.line('class GraphQL%s(GrappleType):' % grapple_type.name())
    writer.increase_indent() # begin class definition
    writer.line('@staticmethod')
    writer.line('def create_type():')
    writer.increase_indent() # begin create_type body
    writer.line('return GraphQLEnumType(')
    writer.increase_indent() # begin GraphQLEnumType .ctor args
    writer.line("name='%s'," % grapple_type.name())
    writer.line('values={')
    writer.increase_indent() # begin value declarations
    for value in grapple_type.values():
        writer.line("'%s': GraphQLEnumValue()," % value)
    writer.decrease_indent() # end value declarations
    writer.line('},')
    writer.decrease_indent() # end GraphQLEnumType.ctor args
    writer.line(')')

    writer.decrease_indent() # end create_type body
    writer.decrease_indent() # end class definition
    writer.blank_line()

def print_graphql_object_type(writer, document_ast, grapple_type):
    writer.line('class GraphQL%s(GrappleType):' % grapple_type.name())
    writer.increase_indent() # begin class definition
    writer.line('@staticmethod')
    writer.line('def create_type():')
    writer.increase_indent() # begin create_type body
    writer.line('return GraphQLObjectType(')
    writer.increase_indent() # begin GraphQLObjectType .ctor args
    writer.line("name='%s'," % grapple_type.name())
    writer.line('fields=lambda: {')
    writer.increase_indent() # begin field declarations
    for field in grapple_type.fields():
        print_graphql_field(writer, document_ast, field)
    writer.decrease_indent() # end field declarations
    writer.line('},')
    writer.decrease_indent() # end GraphQLObjectType .ctor args
    writer.line(')')
    writer.decrease_indent() # end create_type body
    writer.decrease_indent() # end class definition
    writer.blank_line()

def type_instantiation(type_string):
    lookup = {
        'String': 'GraphQLString',
        'Int': 'GraphQLInt',
        'ID': 'GraphQLID',
    }
    if type_string in lookup:
        return lookup[type_string]
    else:
        return 'GraphQL%s.type()' % type_string

def type_ref_string(type_ref):
    if type_ref.is_list():
        type_ctor = 'list_of(%s)' % type_ref_string(type_ref.list_type())
    else:
        type_ctor = type_instantiation(type_ref.graphql_type())

    if type_ref.is_nullable():
        return type_ctor
    else:
        return 'req(%s)' % type_ctor

def graphql_type_string(type_ref):
    graphql_type = ''
    if type_ref.is_list():
        inner_type = graphql_type_string(type_ref.list_type())
        graphql_type = '[%s]' % inner_type
    else:
        graphql_type = type_ref.graphql_type()

    if type_ref.is_nullable():
        return graphql_type
    else:
        return graphql_type + '!'

def print_graphql_field(writer, document_ast, grapple_field):
    type_ref_str = type_ref_string(grapple_field.type_ref())
    is_enum = document_ast.is_enum(grapple_field.type_ref().graphql_type())
    is_simple = grapple_field.is_bare_field() and not is_enum
    if is_simple:
        writer.line("'%s': GraphQLField(type=%s)," % (grapple_field.name(), type_ref_str))
        return

    writer.line("'%s': GraphQLField(" % grapple_field.name())
    writer.increase_indent() # begin args to GraphQLField .ctor
    writer.line('type=%s,' % type_ref_str)

    if grapple_field.args():
        writer.line('args={')
        writer.increase_indent() # begin entries in args dictionary
        for grapple_arg in grapple_field.args():
            arg_type_ref_str = type_ref_string(grapple_arg.type_ref())
            writer.line("'%s': GraphQLArgument(type=%s)," % (grapple_arg.name(), arg_type_ref_str))

        writer.decrease_indent() # end entries in args dictionary
        writer.line('},') # close args dictionary

    python_name = grapple_field.python_name()
    if is_enum:
        writer.line('resolver=lambda obj, args, *_: obj.%s(*args).name if obj.%s(*args) else None,'
                    % (python_name, python_name))
    elif grapple_field.name_requires_lambda():
        writer.line('resolver=lambda obj, args, *_: obj.%s(*args),' % python_name)

    writer.decrease_indent() # end args to GraphQLField .ctor
    writer.line('),') # close GraphQLField .ctor

def print_graphql_input_field(writer, grapple_field):
    type_ref_str = type_ref_string(grapple_field.type_ref())
    writer.line("'%s': GraphQLInputObjectField(type=%s)," % (grapple_field.name(), type_ref_str))

def graphql_type_to_python_type(graphql_type):
    scalars = {
        'ID' : UUID,
        'Int' : int,
        'Float' : float,
        'String' : str,
        'Boolean' : bool,
        'DateTime' : datetime,
    }
    if graphql_type in scalars:
        return scalars[graphql_type].__name__
    return graphql_type

class GrappleDocument:
    def __init__(self, *, types):
        self._types = types

    def object_types(self):
        return [t for t in self._types if t.is_object()]

    def input_types(self):
        return [t for t in self._types if t.is_input()]

    def enum_types(self):
        return [t for t in self._types if t.is_enum()]

    def is_enum(self, name):
        ttype = self.type_named(name)
        return ttype and ttype.is_enum()

    def type_named(self, name):
        for ttype in self._types:
            if ttype.name() == name:
                return ttype

class TypeVarietal(Enum):
    OBJECT = auto()
    INPUT = auto()
    ENUM = auto()

class GrappleTypeDefinition:
    def __init__(self, *, name, fields, generate_pent, values=None, type_varietal):
        self._name = name
        self._fields = fields
        self._generate_pent = generate_pent
        self._type_varietal = type_varietal
        self._values = values

    @staticmethod
    def object_type(*, name, fields, generate_pent):
        return GrappleTypeDefinition(
            name=name,
            fields=fields,
            generate_pent=generate_pent,
            type_varietal=TypeVarietal.OBJECT)

    @staticmethod
    def input_type(*, name, fields):
        return GrappleTypeDefinition(
            name=name,
            fields=fields,
            generate_pent=False,
            type_varietal=TypeVarietal.INPUT)

    @staticmethod
    def enum_type(*, name, values):
        return GrappleTypeDefinition(
            name=name,
            fields=[],
            generate_pent=False,
            values=values,
            type_varietal=TypeVarietal.ENUM)

    def name(self):
        return self._name

    def has_field(self, name):
        for field in self._fields:
            if field.name() == name:
                return True
        return False 

    def fields(self):
        return self._fields

    def values(self):
        return self._values

    def generate_pent(self):
        return self._generate_pent

    def is_object(self):
        return self._type_varietal == TypeVarietal.OBJECT

    def is_input(self):
        return self._type_varietal == TypeVarietal.INPUT

    def is_enum(self):
        return self._type_varietal == TypeVarietal.ENUM

class GrappleField:
    def __init__(self, *, name, grapple_type_ref, args):
        self._name = name
        self._grapple_type_ref = grapple_type_ref
        self._args = args
        self._name_requires_lambda = name == 'id' or is_camel_case(name)

    def is_bare_field(self):
        return len(self._args) == 0 and not self.name_requires_lambda()

    def name(self):
        return self._name

    def name_requires_lambda(self):
        return self._name_requires_lambda

    def python_name(self):
        if self.name() == 'id':
            return 'obj_id'
        if is_camel_case(self.name()):
            return to_snake_case(self.name())
        return self.name()

    def type_ref(self):
        return self._grapple_type_ref

    def args(self):
        return self._args

def filter_nodes(nodes, ast_cls):
    return filter(lambda node: isinstance(node, ast_cls), nodes)

def has_generate_pent_directive(type_ast):
    return 'generatePent' in [dir_node.name.value for dir_node in type_ast.directives]

def create_grapple_type_definition(type_ast):
    if isinstance(type_ast, ObjectTypeDefinition):
        return create_grapple_object_type(type_ast)
    elif isinstance(type_ast, InputObjectTypeDefinition):
        return create_grapple_input_type(type_ast)
    elif isinstance(type_ast, EnumTypeDefinition):
        return create_grapple_enum_type(type_ast)
    else:
        raise Exception('node not supported: ' + str(type_ast))

def create_grapple_enum_type(enum_type_ast):
    grapple_type_name = enum_type_ast.name.value

    values = [value_ast.name.value for value_ast in enum_type_ast.values]
    return GrappleTypeDefinition.enum_type(
        name=grapple_type_name,
        values=values,
    )

def create_grapple_input_type(input_type_ast):
    grapple_type_name = input_type_ast.name.value
    grapple_fields = [create_grapple_input_field(field) for field in input_type_ast.fields]
    return GrappleTypeDefinition.input_type(
        name=grapple_type_name,
        fields=grapple_fields,
    )

def create_grapple_object_type(object_type_ast):
    grapple_type_name = object_type_ast.name.value
    grapple_fields = [create_grapple_field(field) for field in object_type_ast.fields]
    generate_pent = has_generate_pent_directive(object_type_ast)
    return GrappleTypeDefinition.object_type(
        name=grapple_type_name,
        fields=grapple_fields,
        generate_pent=generate_pent,
    )

def to_snake_case(camel_case):
    with_underscores = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_case)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', with_underscores).lower()

def is_camel_case(string):
    return re.search('[A-Z]', string)

class GrappleFieldArgument:
    def __init__(self, *, name, type_ref):
        self._name = name
        self._type_ref = type_ref

    def name(self):
        return self._name

    def type_ref(self):
        return self._type_ref

def create_grapple_field_arg(graphql_arg):
    if graphql_arg.default_value:
        raise Exception('default_value not supported right now')
    return GrappleFieldArgument(
        name=graphql_arg.name.value,
        type_ref=create_grapple_type_ref(graphql_arg.type),
    )

def create_grapple_input_field(graphql_field):
    return GrappleField(
        name=graphql_field.name.value,
        grapple_type_ref=create_grapple_type_ref(graphql_field.type),
        args=[],
    )

def create_grapple_field(graphql_field):
    return GrappleField(
        name=graphql_field.name.value,
        grapple_type_ref=create_grapple_type_ref(graphql_field.type),
        args=[create_grapple_field_arg(graphql_arg) for graphql_arg in graphql_field.arguments]
    )

class GrappleTypeRef:
    def __init__(self, *, graphql_type=None, python_type=None, is_nullable, is_list=False, list_type=None):
        param_check(is_nullable, bool, 'is_nullable')
        param_check(is_list, bool, 'is_list')
        self._graphql_type = graphql_type
        self._python_type = python_type
        self._is_nullable = is_nullable
        self._is_list = is_list
        self._list_type = list_type

    @staticmethod
    def create_list_ref(*, list_type, is_nullable):
        return GrappleTypeRef(is_list=True, list_type=list_type, is_nullable=is_nullable)

    def graphql_type(self):
        return self._graphql_type

    def python_type(self):
        return self._python_type

    def is_nullable(self):
        return self._is_nullable

    def is_list(self):
        return self._is_list

    def list_type(self):
        return self._list_type

def create_grapple_type_ref(graphql_type_ast):
    if isinstance(graphql_type_ast, NamedType):
        graphql_type_name = graphql_type_ast.name.value
        return GrappleTypeRef(
            graphql_type=graphql_type_name,
            python_type=graphql_type_to_python_type(graphql_type_name),
            is_nullable=True
        )
    elif isinstance(graphql_type_ast, NonNullType) and isinstance(graphql_type_ast.type, NamedType):
        core_graphql_type = graphql_type_ast.type.name.value
        return GrappleTypeRef(
            graphql_type=core_graphql_type,
            python_type=graphql_type_to_python_type(core_graphql_type),
            is_nullable=False
        )
    elif isinstance(graphql_type_ast, ListType):
        return GrappleTypeRef.create_list_ref(
            list_type=create_grapple_type_ref(graphql_type_ast.type),
            is_nullable=True,
        )
    elif isinstance(graphql_type_ast, NonNullType) and isinstance(graphql_type_ast.type, ListType):
        return GrappleTypeRef.create_list_ref(
            list_type=create_grapple_type_ref(graphql_type_ast.type.type),
            is_nullable=False,
        )

    raise Exception('not supported')

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
    print_is_input_data_valid(writer, grapple_type)
    print_generated_fields(writer, grapple_type.fields())
    writer.decrease_indent() # end class definition

def print_if_return_false(writer, if_line):
    writer.line(if_line)
    writer.increase_indent()
    writer.line('return False')
    writer.decrease_indent()

def print_is_input_data_valid(writer, grapple_type):
    writer.line('@staticmethod')
    writer.line('# This method checks to see that data coming out of the database is valid')
    writer.line('def is_input_data_valid(data):')
    writer.increase_indent()
    writer.line("raise Exception('must implement in manual class')")
    writer.decrease_indent()
    writer.blank_line()

    # writer.increase_indent() # begin is_input_data_valid implementation
    # print_if_return_false(writer, 'if not isinstance(data, dict):')
    # for field in grapple_type.fields():
    #     type_ref = field.type_ref()
    #     if type_ref.is_nullable():
    #         print_optional_data_check(writer, field)
    #     else:
    #         print_required_data_check(writer, field)

    # writer.line('return True')
    # writer.decrease_indent() # end is_input_data_valid implementation
    # writer.blank_line()

def print_required_data_check(writer, field):
    python_type = field.type_ref().python_type()
    graphql_type = graphql_type_string(field.type_ref())
    print_if_return_false(
        writer,
        "if req_data_elem_invalid(data, '%s', %s): # %s: %s" %
        (field.python_name(), python_type, field.name(), graphql_type)
    )

def print_optional_data_check(writer, field):
    python_type = field.type_ref().python_type()
    graphql_type = graphql_type_string(field.type_ref())
    print_if_return_false(
        writer,
        "if opt_data_elem_invalid(data, '%s', %s): # %s: %s" %
        (field.name(), python_type, field.name(), graphql_type)
    )

def print_generated_fields(writer, fields):
    for field in fields:
        writer.line('def %s(self):' % field.python_name())
        writer.increase_indent() # begin property implemenation
        if field.type_ref().is_nullable():
            writer.line("return self._data.get('%s')" % field.python_name())
        else:
            writer.line("return self._data['%s']" % field.python_name())
        writer.decrease_indent() # end property definition
        writer.blank_line()
