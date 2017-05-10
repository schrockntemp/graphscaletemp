from graphscale.pent import Pent, PentConfig, create_pent
from graphscale.utils import param_check

class Hospital(Pent):
    @staticmethod
    # This method checks to see that data coming out of the database is valid
    def is_input_data_valid(_data):
        return True

    def provider_number(self):
        return self._data['provider']

class CreateHospitalInput:
    def __init__(self, *, provider):
        param_check(provider, str, 'provider')
        self._data = {
            'provider' : provider,
        }

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

