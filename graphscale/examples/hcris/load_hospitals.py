import sys
import csv

import pymysql
import pymysql.cursors

from graphscale.kvetch.kvetch import Kvetch
from graphscale.kvetch.kvetch_dbshard import (
    KvetchDbShard,
    KvetchDbSingleConnectionPool,
    KvetchDbIndexDefinition,
)
from graphscale.kvetch.kvetch_dbschema import (
    init_shard_db_tables,
    drop_shard_db_tables,
)

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

    index = KvetchDbIndexDefinition(
        indexed_attr='provider',
        indexed_sql_type='CHAR(255)',
        index_name='provider_index',
    )
    drop_shard_db_tables(shards[0], {})
    init_shard_db_tables(shards[0], {})
    return Kvetch(shards=shards, edges=[], indexes=[index])

async def load_objects():
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
            new_id = await kvetch.gen_insert_object(type_id, data)
            print('inserted ' + str(new_id))

async def test_index_lookup():
    kvetch = get_kvetch()
    index = kvetch.get_index('provider_index')
    objs = await kvetch.gen_from_index(index, '100002')
    print(objs)

if __name__ == '__main__':
    execute_gen(load_objects())
    # execute_gen(test_index_lookup())