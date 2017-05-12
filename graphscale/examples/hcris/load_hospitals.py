import sys
import csv

import pymysql
import pymysql.cursors

from graphscale.kvetch import sync
from graphscale.kvetch.kvetch_utils import data_to_body, body_to_data

from collections import OrderedDict

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

async def create_providers(filename):
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
    # report_count = {}
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

            # provider = data['prvdr_num']
            # if provider not in report_count:
            #     report_count[provider] = 1
            # else:
            #     report_count[provider] += 1


async def test_index_lookup():
    kvetch = create_kvetch()
    index = kvetch.get_index('provider_index')
    objs = await kvetch.gen_from_index(index, '100002')
    print(objs)

async def create_nmrc(filename):
    expected_header = ['rpt_rec_num', 'wksht_cd', 'line_num', 'clmn_num', 'itm_val_num']
    with open(filename, 'r') as csvfile:
        row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = next(row_reader)
        if header != expected_header:
            raise Exception('headers not expected')

        rec_num_dict = {}
        count = 0
        linecolset = set()
        for data_row in row_reader:
            data = dict(zip(header, data_row))
            report_num = data['rpt_rec_num']
            if report_num not in rec_num_dict:
                rec_num_dict[report_num] = []
            rec_num_dict[report_num].append(data)
            count += 1
            linecolset.add(data['line_num'] + ':' + data['clmn_num'])

        # total_bytes = 0
        # for report_num, recs in rec_num_dict.items():
        #     print('report_num: %s len: %s' % (report_num, len(recs)))
        #     new_data = {'recs' : recs}
        #     total_bytes += len(data_to_body(new_data))

        print(sorted(list(linecolset)))
        print('length: ' + str(len(linecolset)))
        # print('total bytes: ' + str(total_bytes))

def try_parse_int(val):
    try:
        int(val)
        return True
    except Exception as e:
        return False

def get_num_and_sub(string):
    return (string[0:3], int(string[-2:]))

def chopzeros(string):
    if string.startswith('0'):
        return string[1:]
    if string.startswith('00'):
        return string[2:]
    return string

def prettysub(string):
    major, minor = get_num_and_sub(string)
    major = chopzeros(major)
    if minor == 0:
        return str(major)
    return str(major) + '.' + str(minor)
    
def safe_add(matrix, line, column, value):
    if line not in matrix:
        matrix[line] = OrderedDict()
    matrix[line][column] = value

def process_nmrc(filename):
    expected_header = ['rpt_rec_num', 'wksht_cd', 'line_num', 'clmn_num', 'itm_val_num']
    records = []
    with open(filename, 'r') as csvfile:
        row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = next(row_reader)
        if header != expected_header:
            raise Exception('headers not expected')
        for data_row in row_reader:
            data = dict(zip(header, data_row))
            records.append((
                data['rpt_rec_num'],
                data['wksht_cd'],
                prettysub(data['line_num']),
                prettysub(data['clmn_num']),
                data['itm_val_num'],
            ))

    return records

def process_alpha(filename):
    expected_header = ["rpt_rec_num","wksht_cd","line_num","clmn_num","alphnmrc_itm_txt"]
    records = []
    with open(filename, 'r') as csvfile:

        row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = next(row_reader)
        if header != expected_header:
            raise Exception('headers not expected')
        for data_row in row_reader:
            data = dict(zip(header, data_row))
            records.append((
                data['rpt_rec_num'],
                data['wksht_cd'],
                prettysub(data['line_num']),
                prettysub(data['clmn_num']),
                data['alphnmrc_itm_txt'],
            ))
    return records

        # worksheets = set()
        # sets = {}
        # matrix = OrderedDict()
        # for data_row in row_reader:
        #     data = dict(zip(header, data_row))
        #     report_num = data['rpt_rec_num']
        #     worksheet = data['wksht_cd']
        #     line_num = data['line_num']
        #     column_num = data['clmn_num']
        #     value = data['alphnmrc_itm_txt']
        #     if worksheet == 'A800000' and report_num == '577257':
        #         safe_add(matrix, line_num, column_num, value)
        #         key = (worksheet, line_num, column_num)
        #         if key not in sets:
        #             sets[key] = set()
        #         sets[key].add(value)

        # for line, columns in matrix.items():
        #     for column, value in columns.items():
        #         print('%s:%s => %s' % (prettysub(line), prettysub(column), value))

def build_worksheet(worksheet, report_num, records):
    matrix = OrderedDict()
    for record in records:
        if record[0] == report_num and record[1] == worksheet:
            _, _, line, column, value = record
            safe_add(matrix, line, column, value)

    for line, columns in matrix.items():
        chunks_to_print = []
        for column, value in columns.items():
            chunks_to_print.append('%s:%s => %s' % (line, column, value))
        print(', '.join(chunks_to_print))

def main_process():
    records = process_alpha('/Users/schrockn/data/hcris/2552-10/2016/hosp_alpha2552_10_2016_long.csv')
    records.extend(process_nmrc('/Users/schrockn/data/hcris/2552-10/2016/hosp_nmrc2552_10_2016_long.csv'))
    build_worksheet('A800000', '577257', records)

# from timeit import default_timer as timer
# start = timer()
# end = timer()
if __name__ == '__main__':
    # ~/data/hcris/providers/addr2552_10.csv
    execute_gen(create_providers('/Users/schrockn/data/hcris/providers/addr2552_10.csv'))
    # ~/data/hcris/2552-10/2016/hosp_rpt2552_10_2016.csv
    execute_gen(create_reports('/Users/schrockn/data/hcris/2552-10/2016/hosp_rpt2552_10_2016.csv'))
    # /Users/schrockn/data/hcris/2552-10/2016/hosp_nmrc2552_10_2016_long.csv
    # execute_gen(create_nmrc('/Users/schrockn/data/hcris/2552-10/2016/hosp_nmrc2552_10_2016_long.csv'))
    #main_process()

