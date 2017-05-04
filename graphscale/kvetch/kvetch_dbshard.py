import pickle
import zlib
from datetime import datetime
from uuid import UUID

from pymysql.connections import Connection

from graphscale.utils import (
    execute_coro,
    param_check,
)

from graphscale.kvetch.kvetch import (
    KvetchShard,
    KvetchIndexDefinition,
    KvetchEdgeDefinition,
)

def data_to_body(data):
    return zlib.compress(pickle.dumps(data))

def body_to_data(body):
    if body is None:
        return {}
    return pickle.loads(zlib.decompress((body)))

def row_to_obj(row):
    id_dict = {'id' : UUID(bytes=row['id']), '__type_id' : row['type_id']}
    body_dict = body_to_data(row['body'])
    return {**id_dict, **body_dict}

def _kv_shard_get_object(shard_conn, id_):
    obj_dict = _kv_shard_get_objects(shard_conn, [id_])
    return obj_dict[id_]

def _kv_shard_get_objects(shard_conn, ids):
    values_sql = ', '.join(['%s' for x in range(0, len(ids))])
    sql = 'SELECT id, type_id, body from kvetch_objects where id in (' + values_sql + ')'
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, [id_.bytes for id_ in ids])
        rows = cursor.fetchall()

    ids_out = [UUID(bytes=row['id']) for row in rows]
    obj_list = [row_to_obj(row) for row in rows]
    return dict(zip(ids_out, obj_list))

def _kv_shard_insert_object(shard_conn, new_id, type_id, data):
    with shard_conn.cursor() as cursor:
        sql = 'INSERT INTO kvetch_objects(id, type_id, updated, body) VALUES (%s, %s,  %s, %s)'
        cursor.execute(sql, (new_id.bytes, type_id, datetime.now(), data_to_body(data)))

    shard_conn.commit()
    return new_id

def _to_sql_value(value):
    direct_types = (datetime, int, str)
    if isinstance(value, UUID):
        return value.bytes
    if isinstance(value, direct_types):
        return value
    raise Exception('type not supported yet: ' + str(type(value)))

def _kv_shard_insert_index_entry(
        shard_conn,
        index_name,
        index_column,
        index_value,
        target_column,
        target_id):

    sql = 'INSERT INTO %s (%s, %s, updated)' % (index_name, index_column, target_column)
    sql += ' VALUES(%s, %s, %s)'
    values = [index_value, target_id, datetime.now()]
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, tuple(_to_sql_value(v) for v in values))
    shard_conn.commit()

def _kv_shard_insert_edge(
        shard_conn,
        edge_id,
        from_id,
        to_id,
        data):
    sql = 'INSERT into kvetch_edges (edge_id, from_id, to_id, body) VALUES(%s, %s, %s, %s)'
    values = (edge_id, from_id.bytes, to_id.bytes, data_to_body(data))
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, values)
    shard_conn.commit()

def _kv_shard_get_edges(
        shard_conn,
        edge_id,
        from_id):
    sql = 'SELECT from_id, to_id, body FROM kvetch_edges WHERE from_id = %s'
    rows = []
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, from_id.bytes)
        rows = cursor.fetchall()

    edges = []
    for row in rows:
        edges.append({
            'from_id': UUID(bytes=row['from_id']),
            'to_id': UUID(bytes=row['to_id']),
            'data': body_to_data(row['body']),
        })
    return edges

def _kv_shard_get_index_entries(shard_conn, index_name, index_column, index_value, target_column):
    sql = 'SELECT %s FROM %s WHERE %s =' % (target_column, index_name, index_column)
    sql += '%s'
    sql += ' ORDER BY %s' % target_column
    rows = []
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, (_to_sql_value(index_value)))
        rows = cursor.fetchall()

    for row in rows:
        row['target_id'] = UUID(bytes=row['target_id'])
    return rows


class KvetchDbSingleConnectionPool:
    def __init__(self, conn):
        param_check(conn, Connection, 'conn')
        self._conn = conn

    def conn(self):
        return self._conn

def sync_kv_insert_object(shard, new_id, type_id, data):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_insert_object(new_id, type_id, data))

def sync_kv_get_object(shard, id_):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_object(id_))

def sync_kv_get_objects(shard, ids):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_objects(ids))

# def sync_kv_index_get_all(shard, index, value):
#     param_check(shard, KvetchShard, 'shard')
#     param_check(index, KvetchShardIndex, 'index')
#     return execute_coro(shard.gen_all(index, value))

def sync_kv_insert_index_entry(shard, index_name, index_value, target_id):
    return execute_coro(shard.gen_insert_index_entry(index_name, index_value, target_id))

def sync_kv_insert_edge(shard, edge_def, from_id, to_id, data=None):
    return execute_coro(shard.gen_insert_edge(edge_def, from_id, to_id, data))

def sync_kv_get_index_entries(shard, index, index_value):
    return execute_coro(shard.gen_index_entries(index, index_value))

def sync_kv_get_index_ids(shard, index, index_value):
    entries = sync_kv_get_index_entries(shard, index, index_value)
    return [entry['target_id'] for entry in entries]

def sync_kv_get_edge_ids(shard, edge_def, from_id):
    return execute_coro(shard.gen_edge_ids(edge_def, from_id))

class KvetchDbEdgeDefinition(KvetchEdgeDefinition):
    pass

class KvetchDbIndexDefinition(KvetchIndexDefinition):
    def __init__(self, *, indexed_attr, indexed_sql_type, index_name):
        param_check(indexed_attr, str, 'indexed_attr')
        param_check(indexed_sql_type, str, 'indexed_sql_type')
        param_check(index_name, str, 'index_name')

        self._indexed_attr = indexed_attr
        self._indexed_sql_type = indexed_sql_type
        self._index_name = index_name

    def index_name(self):
        return self._index_name

    def indexed_attr(self):
        return self._indexed_attr

    def indexed_sql_type(self):
        return self._indexed_sql_type

class KvetchDbShard(KvetchShard):
    def __init__(self, *, pool):
        self._pool = pool

    @staticmethod
    def fromconn(conn):
        return KvetchDbShard(pool=KvetchDbSingleConnectionPool(conn)) 

    def conn(self):
        return self._pool.conn()

    async def gen_object(self, id_):
        param_check(id_, UUID, 'id_')
        return _kv_shard_get_object(self.conn(), id_)

    async def gen_objects(self, ids):
        param_check(ids, list, 'ids')
        if not ids:
            raise ValueError('ids must have at least 1 element')
        return _kv_shard_get_objects(self.conn(), ids)

    async def gen_insert_index_entry(self, index, index_value, target_id):
        attr = index.indexed_attr()
        _kv_shard_insert_index_entry(
            shard_conn=self.conn(),
            index_name=index.index_name(),
            index_column=attr,
            index_value=index_value,
            target_column='target_id',
            target_id=target_id,
        )

    async def gen_insert_edge(self, edge_definition, from_id, to_id, data=None):
        param_check(from_id, UUID, 'from_id')
        param_check(to_id, UUID, 'to_id')
        if data is None:
            data = {}
        param_check(data, dict, 'data')
        _kv_shard_insert_edge(self.conn(), edge_definition.edge_id(), from_id, to_id, data)

    async def gen_insert_object(self, new_id, type_id, data):
        self.check_insert_object_vars(new_id, type_id, data)
        _kv_shard_insert_object(self.conn(), new_id, type_id, data)
        return new_id

    async def gen_edges(self, edge_definition, from_id):
        param_check(from_id, UUID, 'from_id')
        return _kv_shard_get_edges(self.conn(), edge_definition.edge_id(), from_id)

    async def gen_edge_ids(self, edge_definition, from_id):
        edges = await self.gen_edges(edge_definition, from_id)
        return [edge['to_id'] for edge in edges]

    async def gen_index_entries(self, index, value):
        return _kv_shard_get_index_entries(
            shard_conn=self.conn(),
            index_name=index.index_name(),
            index_column=index.indexed_attr(),
            index_value=value,
            target_column='target_id',
        )
