from graphscale.pent import Pent, PentConfig, create_pent
from graphscale.utils import param_check

from datetime import date

from enum import Enum, auto

class HospitalStatus(Enum):
    AS_SUBMITTED = auto()
    SETTLED = auto()
    AMENDED = auto()
    SETTLED_WITH_AUDIT = auto()
    REOPENED = auto()

class Hospital(Pent):
    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(_data):
        return True

    def provider_number(self):
        return int(self._data['provider'])

    def fiscal_year_begin(self):
        parts = self._data['fyb'].split('/')
        return date(int(parts[2]), int(parts[0]), int(parts[1]))

    def fiscal_year_end(self):
        parts = self._data['fye'].split('/')
        return date(int(parts[2]), int(parts[0]), int(parts[1]))

    def status(self):
        lookup = {
            'As Submitted': HospitalStatus.AS_SUBMITTED,
            'Settled': HospitalStatus.SETTLED,
            'Amended': HospitalStatus.AMENDED,
            'Settled w/Audit': HospitalStatus.SETTLED_WITH_AUDIT,
            'Reopened': HospitalStatus.REOPENED,
        }
        status_enum = lookup[self._data['status']]
        return status_enum

    def hospital_name(self):
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
class CreateHospitalInput:
    def __init__(self, data):
        self._data = data

    def data(self):
        return self._data

def get_hcris_edge_config():
    return {}

def get_hcris_object_config():
    return {100000: Hospital}

def get_hcris_config():
    return PentConfig(object_config=get_hcris_object_config(), edge_config=get_hcris_edge_config())

async def create_hospital(context, input_object):
    return await create_pent(context, Hospital, input_object)

