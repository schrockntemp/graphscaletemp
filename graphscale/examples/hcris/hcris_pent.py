from graphscale.pent import Pent, PentConfig, create_pent
from graphscale.utils import param_check

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

class Report(Pent):
    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(_data):
        return True

    def report_record_number(self):
        return int(self._data['rpt_rec_num'])

    def provider_number(self):
        return int(self._data['prvdr_num'])

    async def gen_hospital(self):
        pass

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

# Consider if this is going to be a necessary abstraction
class ProviderCsvRow:
    def __init__(self, data):
        self._data = data

    def data(self):
        return self._data

def get_hcris_edge_config():
    return {}

def get_hcris_object_config():
    return {100000: Provider}

def get_hcris_config():
    return PentConfig(object_config=get_hcris_object_config(), edge_config=get_hcris_edge_config())

async def create_provider(context, input_object):
    return await create_pent(context, Provider, input_object)

