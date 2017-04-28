from graphscale.utils import execute_coro

from graphscale.kvetch.kvetch_dbshard import (
    data_to_body,
    body_to_data,
    KvetchDbShard,
)

from uuid import UUID

from graphscale.utils import (
    param_check
)

# task: add separate concepts of index and edges

class KvetchContext:
    def __init__(self, conn):
        self._conn = conn

    def conn(self):
        return self._conn


async def kv_gen_object(context, id_):
    if context is None:
        raise ValueError('dld')
    shard = KvetchDbShard.fromconn(context.conn())
    return await shard.gen_object(id_)

def kv_get_object_sync(context, id_):
    return execute_coro(kv_gen_object(context, id_))

async def kv_gen_objects(context, ids):
    if context is None:
        raise ValueError('dld')
    shard = KvetchDbShard.fromconn(context.conn())
    return await shard.gen_objects(ids)

def kv_get_objects_sync(context, ids):
    param_check(context, KvetchContext, 'context')
    param_check(ids, list, 'ids')
    if not ids:
        raise ValueError('ids must have at least 1 element')

    return execute_coro(kv_gen_objects(context, ids))

# def kv_insert_object(context, type_id, data):
#     if context is None:
#         raise ValueError('dld')
#     shard = KvetchDbShard.fromconn(context.conn())
#     return execute_coro(shard.gen_insert_object(type_id, data))

def index_table_name(index_name):
    return 'kvetch_%s_edges' % index_name

def kv_insert_index_entry(context, index_name, id_from, id_to, data=None):
    if data is None:
        data = {}
    param_check(context, KvetchContext, 'context')
    param_check(index_name, str, 'index_name')
    param_check(id_from, UUID, 'id_from')
    param_check(id_to, UUID, 'id_to')
    param_check(data, dict, 'data')

    conn = context.conn()
    with conn.cursor() as cursor:
        sql = ('INSERT INTO ' + index_table_name(index_name) +
               '(id_from, id_to, body) VALUES(%s, %s, %s) ')
        cursor.execute(sql, (id_from.bytes, id_to.bytes, data_to_body(data)))
    conn.commit()

async def kv_gen_index_entries(context, index_name, id_from):
    return kv_get_index_entries(context, index_name, id_from)

def kv_get_index_entries(context, index_name, id_from):
    param_check(context, KvetchContext, 'context')
    param_check(index_name, str, 'index_name')
    param_check(id_from, UUID, 'id_from')
    conn = context.conn()
    with conn.cursor() as cursor:
        sql = ('SELECT id_from, id_to, body FROM ' + index_table_name(index_name) +
               ' WHERE id_from = %s ORDER BY updated')
        cursor.execute(sql, (id_from.bytes))
        rows = cursor.fetchall()
        entries = []
        for row in rows:
            entries.append({
                'id_from':UUID(bytes=row['id_from']),
                'id_to':UUID(bytes=row['id_to']),
                'data':body_to_data(row['body']),
            })
    return entries
