#C0301: line too long. csv files
#pylint: disable=W0621,C0103,W0401,W0614,C0301

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
    create_provider,
    ProviderCsvRow,
    Provider,
    ProviderStatus,
    Report,
    MedicareUtilizationLevel,
    get_pent_context,
)

from graphscale.examples.hcris.hcris_db import init_hcris_db_kvetch 

from test.test_utils import MagnusConn, db_mem_fixture

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
        import sys
        sys.stderr.write('GraphQL Error:')
        sys.stderr.write(str(error))
        sys.stderr.write('Original Error:')
        sys.stderr.write(str(error.original_error))
        raise error
    return result

def mem_context():
    shard = KvetchMemShard()
    kvetch = Kvetch(shards=[shard], edges=[], indexes=[])
    return PentContext(kvetch=kvetch, config=get_hcris_config())

def db_context():
    conn = MagnusConn.get_unittest_conn()
    return get_pent_context(init_hcris_db_kvetch(conn))


def create_test_data(header, csv_row):
    reader = csv.reader([csv_row], delimiter=',', quotechar='"')
    values = next(reader)
    return dict(zip(header, values))

def create_test_hospital_data(csv_row):
    header = [
        'provider', 'prvdr_num', 'fyb', 'fybstr', 'fye',
        'fyestr', 'status', 'ctrl_type', 'hosp_name', 'street_addr', 'po_box',
        'city', 'state', 'zip_code', 'county']
    return create_test_data(header, csv_row)

def create_test_report_data(csv_row):
    header = [
        'rpt_rec_num', 'prvdr_ctrl_type_cd', 'prvdr_num', 'rpt_stus_cd',
        'initl_rpt_sw', 'last_rpt_sw', 'trnsmtl_num', 'fi_num',
        'adr_vndr_cd', 'util_cd', 'spec_ind', 'npi', 'fy_bgn_dt', 'fy_end_dt',
        'proc_dt', 'fi_creat_dt', 'npr_dt', 'fi_rcpt_dt'
    ]
    return create_test_data(header, csv_row)

@pytest.fixture(params=db_mem_fixture(mem=mem_context, db=db_context))
def pent_context(request):
    return request.param()

def test_hcris_pent_hospital_massaging(pent_context):
    obj_id = uuid4()
    line = '100001,"100001",7/1/2015,"01-JUL-15",6/30/2016,"30-JUN-16","As Submitted",2,"SHANDS JACKSONVILLE MEDICAL CENTER","655 WEST 8TH STREET",,"JACKSONVILLE","FL","32209-","DUVAL"'
    data = create_test_hospital_data(line)
    hospital = Provider(pent_context, obj_id, data)
    assert hospital.provider_number() == 100001
    assert hospital.fiscal_year_begin() == date(2015, 7, 1)
    assert hospital.fiscal_year_end() == date(2016, 6, 30)
    assert hospital.status() == ProviderStatus.AS_SUBMITTED

def test_browse_hospitals(pent_context):
    graphql_schema = create_hcris_schema()
    line_one = '100002,"100002",10/1/2015,"01-OCT-15",9/30/2016,"30-SEP-16","As Submitted",2,"BETHESDA HOSPITAL  INC","2815 SOUTH SEACREST BLVD",,"BOYNTON BEACH","FL","33435","PALM BEACH"'
    line_two = '100006,"100006",10/1/2015,"01-OCT-15",9/30/2016,"30-SEP-16","As Submitted",2,"ORLANDO HEALTH","1414  KUHL AVENUE",,"ORLANDO","FL","32806","ORANGE"'
    line_three = '100007,"100007",1/1/2015,"01-JAN-15",12/31/2015,"31-DEC-15","Amended",2,"FLORIDA HOSPITAL","601 EAST ROLLINS STREET",,"ORLANDO","FL","32803","ORANGE"'
    datas = [
        persist_hospital_from_csv(line_one, pent_context, graphql_schema),
        persist_hospital_from_csv(line_two, pent_context, graphql_schema),
        persist_hospital_from_csv(line_three, pent_context, graphql_schema),
    ]

    ids = [UUID(hex=data['createProvider']['id']) for data in datas]
    id_first, id_second, id_third = tuple(sorted(ids))

    all_hospitals = execute_gen(Provider.gen_all(pent_context, None, None))
    all_ids = [hospital.obj_id() for hospital in all_hospitals]
    assert all_ids == [id_first, id_second, id_third]

    after_one_hospitals = execute_gen(Provider.gen_all(pent_context, after=id_first, first=None))
    after_one_ids = [hospital.obj_id() for hospital in after_one_hospitals]
    assert after_one_ids == [id_second, id_third]

    after_one_first_one_hospitals = execute_gen(Provider.gen_all(pent_context, after=id_first, first=1))
    after_one_first_one_ids = [hospital.obj_id() for hospital in after_one_first_one_hospitals]
    assert after_one_first_one_ids == [id_second]

def persist_hospital_from_csv(line, pent_context, graphql_schema):
    data = create_test_hospital_data(line)
    mutation_query = string.Template("""
    mutation {
        createProvider(input: {
            provider: "${provider}"
            fyb: "${fyb}"
            fye: "${fye}"
            status: "${status}"
            ctrl_type: "${ctrl_type}"
            hosp_name: "${hosp_name}"
            street_addr: "${street_addr}"
            po_box: "${po_box}"
            city: "${city}"
            state: "${state}"
            zip_code: "${zip_code}"
            county: "${county}"
        }) {
            id
            providerNumber
            fiscalYearBegin
            fiscalYearEnd
            status
            name 
            streetAddress
            poBox
            city
            state
            zipCode
            county
        }
    }
""").substitute(data)

    result = execute_query(mutation_query, pent_context, graphql_schema)
    return result.data

def test_hcris_row_graphql(pent_context):
    graphql_schema = create_hcris_schema()
    line = '100001,"100001",7/1/2015,"01-JUL-15",6/30/2016,"30-JUN-16","As Submitted",2,"SHANDS JACKSONVILLE MEDICAL CENTER","655 WEST 8TH STREET",,"JACKSONVILLE","FL","32209-","DUVAL"'
    out_data = persist_hospital_from_csv(line, pent_context, graphql_schema)
    del out_data['createProvider']['id']
    assert out_data['createProvider'] == {
        'providerNumber': 100001,
        'fiscalYearBegin': '2015-07-01',
        'fiscalYearEnd': '2016-06-30',
        'status': 'AS_SUBMITTED',
        'name': 'SHANDS JACKSONVILLE MEDICAL CENTER',
        'streetAddress': '655 WEST 8TH STREET',
        'poBox': '',
        'city': 'JACKSONVILLE',
        'state': 'FL',
        'zipCode': '32209',
        'county': 'DUVAL',
    }

def test_hcris_report():
    obj_id = uuid4()
    # type_id = 200000 # report objects
    line = '577257,"2","420011",1,"N","N","G","11001",4,"F",,,10/1/2015,12/31/2015,6/14/2016,6/1/2016,,5/25/2016'
    data = create_test_report_data(line)
    report = Report(mem_context(), obj_id, data)
    assert report.obj_id() == obj_id
    assert report.report_record_number() == 577257
    assert report.provider_number() == 420011
    assert report.fiscal_intermediary_number() == '11001'
    assert report.process_date() == date(2016, 6, 14)
    assert report.medicare_utilization_level() == MedicareUtilizationLevel.FULL
