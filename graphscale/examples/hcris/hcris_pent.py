from graphscale.pent import Pent, PentConfig, create_pent, PentContext
from graphscale.utils import param_check, print_error
from graphscale.kvetch import Kvetch

from datetime import date

from enum import Enum, auto

class ProviderStatus(Enum):
    AS_SUBMITTED = auto()
    SETTLED = auto()
    AMENDED = auto()
    SETTLED_WITH_AUDIT = auto()
    REOPENED = auto()

class MedicareUtilizationLevel(Enum):
    NONE = auto()
    LOW = auto()
    FULL = auto()

def parse_american_date(value):
    parts = value.split('/')
    return date(int(parts[2]), int(parts[0]), int(parts[1]))

async def pent_from_index(context, pent_cls, index_name, value):
    obj_id = await context.kvetch().gen_id_from_index(index_name, value)
    if not obj_id:
        return None
    return await pent_cls.gen(context, obj_id)

class Report(Pent):
    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(data):
        if data['type_id'] != 200000:
            raise Exception('invalid type for Report ' + str(data['type_id']))
        return True

    def report_record_number(self):
        # print_error('IN METHOD Report.report_record_number')
        # print_error(self._data)
        #return int(self._data['report_record_number'])
        return int(self._data['rpt_rec_num'])

    def provider_number(self):
        return int(self._data['prvdr_num'])

    async def provider(self):
        return await Provider.gen(self.context(), self._data['provider_id'])

    def fiscal_intermediary_number(self):
        return self._data['fi_num']

    def process_date(self):
        return parse_american_date(self._data['proc_dt'])

    def medicare_utilization_level(self):
        code = self._data['util_cd']
        if code == 'N':
            return MedicareUtilizationLevel.NONE
        if code == 'L':
            return MedicareUtilizationLevel.LOW
        if code == 'F' or code == '':
            return MedicareUtilizationLevel.FULL

        raise Exception('unexpected code: ' + code)

    async def gen_worksheet_instances(self, after, first):
        edge_name = 'report_to_worksheet_instance'
        return await self.gen_associated_pents(WorksheetInstance, edge_name, after, first)

class Provider(Pent):
    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(data):
        if data['type_id'] != 100000:
            raise Exception('invalid type for Provider ' + str(data['type_id']))
        return True

    async def gen_reports(self, after=None, first=None):
        edge_name = 'provider_to_report'
        return await self.gen_associated_pents(Report, edge_name, after, first)

    def provider_number(self):
        return int(self._data['provider'])

    def fiscal_year_begin(self):
        return parse_american_date(self._data['fyb'])

    def fiscal_year_end(self):
        return parse_american_date(self._data['fye'])

    def status(self):
        lookup = {
            'As Submitted': ProviderStatus.AS_SUBMITTED,
            'Settled': ProviderStatus.SETTLED,
            'Amended': ProviderStatus.AMENDED,
            'Settled w/Audit': ProviderStatus.SETTLED_WITH_AUDIT,
            'Reopened': ProviderStatus.REOPENED,
        }
        status_enum = lookup[self._data['status']]
        return status_enum

    def name(self):
        return self._data['hosp_name']

    def street_address(self):
        return self._data['street_addr']

    def po_box(self):
        return self._data['po_box']

    def city(self):
        return self._data['city']

    def state(self):
        return self._data['state']

    def zip_code(self):
        code = self._data['zip_code']
        if code[-1] == '-':
            return code[:-1]
        return code

    def county(self):
        return self._data['county']

class WorksheetInstance(Pent):
    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(data):
        if data['type_id'] != 300000:
            raise Exception('invalid type for WorksheetInstance' + str(data['type_id']))
        return True

    def report_record_number(self):
        # print_error('IN METHOD WorksheetInstance.report_record_number')
        # print_error(self._data)
        return self._data['report_record_number']

    def worksheet_code(self):
        return self._data['worksheet_code']

    def entries(self):
        entries = []
        for entry_data in self._data['worksheet_entries']:
            line = entry_data['line']
            column = entry_data['column']
            value = entry_data['value']
            entries.append(WorksheetEntry(line, column, value))
        return entries

def get_num_and_sub(string):
    return (string[0:3], string[-2:])

def chopzeros(string):
    if string.startswith('00'):
        return string[2:]
    if string.startswith('0'):
        return string[1:]
    return string

def get_comps(string):
    major, minor = get_num_and_sub(string)
    major = chopzeros(major)
    minor = chopzeros(minor)
    if minor == '':
        return (major, None)
    return (major, minor)

def try_parse_int(value):
    try:
        return int(value)
    except ValueError:
        return None

def try_parse_float(value):
    try:
        return float(value)
    except ValueError:
        return None


class WorksheetEntry:
    def __init__(self, line, column, value):
        self.line, self.subline = get_comps(line)
        self.column, self.subcolumn = get_comps(column)
        self.value = value

# Consider if this is going to be a necessary abstraction
class ProviderCsvRow:
    def __init__(self, data):
        self._data = data

    def data(self):
        return self._data

class ReportCsvRow:
    def __init__(self, data):
        self._data = data

    def provider_id(self, provider_id):
        self._data['provider_id'] = provider_id
        return self

    def data(self):
        return self._data

class CreateWorksheetInstanceInput:
    def __init__(self, data):
        self._data = data

    def report_id(self, report_id):
        self._data['report_id'] = report_id
        return self

    def data(self):
        return self._data

def get_hcris_edge_config():
    return {}

def get_hcris_object_config():
    return {
        100000: Provider,
        200000: Report,
        300000: WorksheetInstance,
    }

def get_hcris_config():
    return PentConfig(object_config=get_hcris_object_config(), edge_config=get_hcris_edge_config())

def get_pent_context(kvetch):
    param_check(kvetch, Kvetch, 'kvetch')
    return PentContext(
        kvetch=kvetch,
        config=get_hcris_config(),
    )

async def create_provider(context, input_object):
    param_check(context, PentContext, 'context')
    param_check(input_object, ProviderCsvRow, 'input_object')
    return await create_pent(context, Provider, input_object)

async def create_report(context, input_object):
    param_check(context, PentContext, 'context')
    param_check(input_object, ReportCsvRow, 'input_object')

    index_name = 'provider_number_index'
    provider_number = input_object.data()['prvdr_num']
    provider = await pent_from_index(context, Provider, index_name, provider_number)
    if provider:
        input_object.provider_id(provider.obj_id())

    return await create_pent(context, Report, input_object)

async def create_worksheet_instance(context, input_object):
    param_check(context, PentContext, 'context')
    param_check(input_object, CreateWorksheetInstanceInput, 'input_object')

    index_name = 'report_record_number_index'
    record_number = input_object.data()['report_record_number']
    report = await pent_from_index(context, Report, index_name, record_number)
    if report:
        input_object.report_id(report.obj_id())

    return await create_pent(context, WorksheetInstance, input_object)
