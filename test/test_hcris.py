#C0301: line too long. csv files 
#pylint: disable=C0301

import asyncio
import pytest
import csv
from uuid import UUID, uuid4
from datetime import date
import string
import traceback

from graphql import graphql
from graphql.execution.executors.asyncio import AsyncioExecutor

from graphscale.examples.hcris.hcris_graphql import create_hcris_schema
from graphscale.examples.hcris.hcris_pent import (
    get_hcris_config,
    create_hospital,
    CreateHospitalInput,
    Hospital,
    HospitalStatus,
)

from graphscale.kvetch import Kvetch
from graphscale.kvetch.kvetch_memshard import KvetchMemShard

from graphscale.pent import PentContext

from graphscale.utils import execute_gen

def execute_query(query, pent_context, graphql_schema):
    loop = asyncio.new_event_loop()
    result = graphql(
        graphql_schema,
        query,
        executor=AsyncioExecutor(loop=loop),
        context_value=pent_context
    )
    if result.errors:
        error = result.errors[0]
        print('GraphQL Error:')
        print(error)
        raise error.original_error
    return result

def mem_context():
    shard = KvetchMemShard()
    kvetch = Kvetch(shards=[shard], edges=[], indexes=[])
    return PentContext(kvetch=kvetch, config=get_hcris_config())

def create_test_hospital_data(csv_row):
    header = [
        'provider', 'prvdr_num', 'fyb', 'fybstr', 'fye',
        'fyestr', 'status', 'ctrl_type', 'hosp_name', 'street_addr', 'po_box',
        'city', 'state', 'zip_code', 'county']
    reader = csv.reader([csv_row], delimiter=',', quotechar='"')
    values = next(reader)
    return dict(zip(header, values))

def test_hcris_pent_massaging():
    obj_id = uuid4()
    line = '100001,"100001",7/1/2015,"01-JUL-15",6/30/2016,"30-JUN-16","As Submitted",2,"SHANDS JACKSONVILLE MEDICAL CENTER","655 WEST 8TH STREET",,"JACKSONVILLE","FL","32209-","DUVAL"'
    data = create_test_hospital_data(line)
    hospital = Hospital(mem_context(), obj_id, data)
    assert hospital.provider_number() == 100001
    assert hospital.fiscal_year_begin() == date(2015, 7, 1)
    assert hospital.fiscal_year_end() == date(2016, 6, 30)
    assert hospital.status() == HospitalStatus.AS_SUBMITTED

def test_hcris_row_graphql():
    graphql_schema = create_hcris_schema()
    pent_context = mem_context()
    line = '100001,"100001",7/1/2015,"01-JUL-15",6/30/2016,"30-JUN-16","As Submitted",2,"SHANDS JACKSONVILLE MEDICAL CENTER","655 WEST 8TH STREET",,"JACKSONVILLE","FL","32209-","DUVAL"'
    data = create_test_hospital_data(line)
    mutation_query = string.Template("""
    mutation {
        createHospital(input: {
            provider: "${provider}"
            fyb: "${fyb}"
            fye: "${fye}"
            status: "${status}"
        }) {
            providerNumber
            fiscalYearBegin
            fiscalYearEnd
            status
        }
    }
""").substitute(data)

    result = execute_query(mutation_query, pent_context, graphql_schema)
    out_data = result.data
    assert out_data['createHospital'] == {
        'providerNumber': 100001,
        'fiscalYearBegin': '2015-07-01',
        'fiscalYearEnd': '2016-06-30',
        'status': 'AS_SUBMITTED',
    }
