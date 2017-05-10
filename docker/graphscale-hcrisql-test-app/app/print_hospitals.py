import sys
import csv

import pymysql
import pymysql.cursors

from graphscale.kvetch.kvetch import Kvetch
from graphscale.kvetch.kvetch_dbshard import (
    KvetchDbShard,
    KvetchDbSingleConnectionPool,
)
# from graphscale.kvetch.kvetch_dbschema import (
#     init_shard_db_tables,
#     drop_shard_db_tables,
# )

from graphscale.utils import execute_gen

def get_kvetch():
    hcrisql_conn = pymysql.connect(
        host='localhost',
        user='magnus',
        password='magnus',
        db='graphscale-hcrisql-db',
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor)

    shards = [KvetchDbShard(
        pool=KvetchDbSingleConnectionPool(hcrisql_conn),
    )]

    # drop_shard_db_tables(shards[0], {})
    # init_shard_db_tables(shards[0], {})
    return Kvetch(shards=shards, edges=[], indexes=[])

async def do_it():
    filename = sys.argv[1]
    expected_header = [
        'provider', 'prvdr_num', 'fyb', 'fybstr', 'fye',
        'fyestr', 'status', 'ctrl_type', 'hosp_name', 'street_addr', 'po_box',
        'city', 'state', 'zip_code', 'county']

    type_id = 100000 # original object

    kvetch = get_kvetch()
    with open(filename, 'r') as csvfile:
        row_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        header = next(row_reader)

        if header != expected_header:
            raise Exception('headers not expected')

        for data_row in row_reader:
            data = dict(zip(header, data_row))
            # await kvetch.gen_insert_object(type_id, data)
            # new_obj = await kvetch.gen_object(new_obj_id)

execute_gen(do_it())
