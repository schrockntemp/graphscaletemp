import sys
import csv

import pymysql
import pymysql.cursors

from graphscale.kvetch import sync

from graphscale.examples.hcris.hcris_db import (
    init_hcris_db_kvetch,
    create_hcris_db_kvetch,
)

from graphscale.utils import execute_gen

def create_kvetch():
    hcrisql_conn = pymysql.connect(
        host='localhost',
        user='magnus',
        password='magnus',
        db='graphscale-hcrisql-db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)
    # return init_hcris_db_kvetch(hcrisql_conn)
    return create_hcris_db_kvetch(hcrisql_conn)

async def create_hospitals(filename):
    expected_header = [
        'provider', 'prvdr_num', 'fyb', 'fybstr', 'fye',
        'fyestr', 'status', 'ctrl_type', 'hosp_name', 'street_addr', 'po_box',
        'city', 'state', 'zip_code', 'county']

    type_id = 100000 # hospital object

    kvetch = create_kvetch()
    with open(filename, 'r') as csvfile:
        row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = next(row_reader)

        if header != expected_header:
            raise Exception('headers not expected')

        count = 0
        for data_row in row_reader:
            data = dict(zip(header, data_row))
            new_id = await kvetch.gen_insert_object(type_id, data)
            print('inserted provider ' + str(new_id) + ' num ' + str(count))
            count += 1


async def create_reports(filename):
    expected_header = [
        'rpt_rec_num', 'prvdr_ctrl_type_cd', 'prvdr_num', 'rpt_stus_cd',
        'initl_rpt_sw', 'last_rpt_sw', 'trnsmtl_num', 'fi_num',
        'adr_vndr_cd', 'util_cd', 'spec_ind', 'npi', 'fy_bgn_dt', 'fy_end_dt',
        'proc_dt', 'fi_creat_dt', 'npr_dt', 'fi_rcpt_dt'
    ]
    type_id = 200000 # report objects
    kvetch = create_kvetch()
    with open(filename, 'r') as csvfile:
        row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = next(row_reader)
        if header != expected_header:
            raise Exception('headers not expected')
        count = 0
        for data_row in row_reader:
            data = dict(zip(header, data_row))
            new_id = await kvetch.gen_insert_object(type_id, data)
            print('inserted report ' + str(new_id) + ' num ' + str(count))
            count += 1

async def test_index_lookup():
    kvetch = create_kvetch()
    index = kvetch.get_index('provider_index')
    objs = await kvetch.gen_from_index(index, '100002')
    print(objs)

if __name__ == '__main__':
    # ~/data/hcris/providers/addr2552_10.csv
    # execute_gen(create_hospitals())
    # ~/data/hcris/2552-10/2016/hosp_rpt2552_10_2016.csv
    execute_gen(create_reports('/Users/schrockn/data/hcris/2552-10/2016/hosp_rpt2552_10_2016.csv'))
