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
    def is_input_data_valid(_data):
        return True

    def report_record_number(self):
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

class Provider(Pent):
    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(_data):
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
    def report_record_number(self):
        return self._data['rpt_rec_num']

    def worksheet_code(self):
        return self._data['wksht_cd']

class WorksheetEntry:
    def line(self):
        pass

    def subline(self):
        pass

    def column(self):
        pass

    def subcolumn(self):
        pass

    def value(self):
        pass

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

def get_hcris_edge_config():
    return {}

def get_hcris_object_config():
    return {
        100000: Provider,
        200000: Report,
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
        print('SETTING provider_id')
        input_object.provider_id(provider.obj_id())

    # get provider_id into report 
    return await create_pent(context, Report, input_object)
