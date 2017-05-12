from datetime import datetime
from uuid import UUID
from collections import OrderedDict

from pymysql.connections import Connection

from graphscale.utils import param_check

from graphscale.kvetch.kvetch import (
    KvetchShard,
    KvetchIndexDefinition,
    KvetchEdgeDefinition,
)

from .kvetch_utils import data_to_body, body_to_data, row_to_obj

class KvetchDbSingleConnectionPool:
    def __init__(self, conn):
        param_check(conn, Connection, 'conn')
        self._conn = conn

    def conn(self):
        return self._conn


class KvetchDbEdgeDefinition(KvetchEdgeDefinition):
    pass

## TODO add index type
class KvetchDbIndexDefinition(KvetchIndexDefinition):
    def __init__(self, *, index_name, indexed_type_id, indexed_attr, sql_type_of_index):
        param_check(index_name, str, 'index_name')
        param_check(indexed_type_id, int, 'indexed_type_id')
        param_check(indexed_attr, str, 'indexed_attr')
        param_check(sql_type_of_index, str, 'sql_type_of_index')

        self._index_name = index_name
        self._indexed_type_id = indexed_type_id
        self._indexed_attr = indexed_attr
        self._sql_type_of_index = sql_type_of_index

    def index_name(self):
        return self._index_name

    def indexed_attr(self):
        return self._indexed_attr

    def sql_type_of_index(self):
        return self._sql_type_of_index

class KvetchDbShard(KvetchShard):
    def __init__(self, *, pool):
        self._pool = pool

    def conn(self):
        return self._pool.conn()

    async def gen_object(self, obj_id):
        param_check(obj_id, UUID, 'obj_id')
        return _kv_shard_get_object(self.conn(), obj_id)

    async def gen_objects(self, ids):
        param_check(ids, list, 'ids')
        if not ids:
            raise ValueError('ids must have at least 1 element')
        return _kv_shard_get_objects(self.conn(), ids)

    async def gen_objects_of_type(self, type_id, after=None, first=None):
        param_check(type_id, int, 'type_id')
        return _kv_shard_get_objects_by_type(self.conn(), type_id, after, first)

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

    async def gen_update_object(self, obj_id, data):
        param_check(obj_id, UUID, 'obj_id')
        param_check(data, dict, 'data')
        old_object = await self.gen_object(obj_id)
        for key, val in data.items():
            old_object[key] = val
        _kv_shard_replace_object(self.conn(), obj_id, data)

    async def gen_delete_object(self, obj_id):
        param_check(obj_id, UUID, 'obj_id')
        _kv_shard_delete_object(self.conn(), obj_id)

    async def gen_edges(self, edge_definition, from_id, after=None, first=None):
        param_check(from_id, UUID, 'from_id')
        return _kv_shard_get_edges(self.conn(), edge_definition.edge_id(), from_id, after, first)

    async def gen_edge_ids(self, edge_definition, from_id, after=None, first=None):
        edges = await self.gen_edges(edge_definition, from_id, after, first)
        return [edge['to_id'] for edge in edges]

    async def gen_index_entries(self, index, value):
        return _kv_shard_get_index_entries(
            shard_conn=self.conn(),
            index_name=index.index_name(),
            index_column=index.indexed_attr(),
            index_value=value,
            target_column='target_id',
        )

def _kv_shard_get_objects_by_type(shard_conn, type_id, after=None, first=None):
    sql = 'SELECT obj_id, type_id, body FROM kvetch_objects WHERE type_id = %s'

    params = [type_id]

    if after:
        sql += ' AND obj_id > %s'
        params.append(after.bytes)

    sql += ' ORDER BY obj_id'

    if first:
        sql += ' LIMIT %s'
        params.append(first)

    with shard_conn.cursor() as cursor:
        cursor.execute(sql, tuple(params))
        rows = cursor.fetchall()

    ids_out = [UUID(bytes=row['obj_id']) for row in rows]
    obj_list = [row_to_obj(row) for row in rows]
    return OrderedDict(zip(ids_out, obj_list))

def _kv_shard_get_object(shard_conn, obj_id):
    obj_dict = _kv_shard_get_objects(shard_conn, [obj_id])
    return obj_dict.get(obj_id)

def _kv_shard_get_objects(shard_conn, ids):
    values_sql = ', '.join(['%s' for x in range(0, len(ids))])
    sql = 'SELECT obj_id, type_id, body FROM kvetch_objects WHERE obj_id in (' + values_sql + ')'
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, [obj_id.bytes for obj_id in ids])
        rows = cursor.fetchall()

    ids_out = [UUID(bytes=row['obj_id']) for row in rows]
    obj_list = [row_to_obj(row) for row in rows]
    return OrderedDict(zip(ids_out, obj_list))

def _kv_shard_insert_object(shard_conn, new_id, type_id, data):
    with shard_conn.cursor() as cursor:
        now = datetime.now()
        sql = 'INSERT INTO kvetch_objects(obj_id, type_id, created, updated, body) '
        sql += 'VALUES (%s, %s, %s, %s, %s)'
        cursor.execute(sql, (new_id.bytes, type_id, now, now, data_to_body(data)))

    shard_conn.commit()
    return new_id

def _kv_shard_replace_object(shard_conn, obj_id, data):
    sql = 'UPDATE kvetch_objects '
    sql += 'SET body = %s, updated = %s '
    sql += 'WHERE obj_id = %s'
    with shard_conn.cursor() as cursor:
        now = datetime.now()
        cursor.execute(sql, (data_to_body(data), now, obj_id.bytes))
    shard_conn.commit()

def _kv_shard_delete_object(shard_conn, obj_id):
    sql = 'DELETE FROM kvetch_objects WHERE obj_id = %s'
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, (obj_id.bytes))
    shard_conn.commit()

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

    sql = 'INSERT INTO %s (%s, %s, created)' % (index_name, index_column, target_column)
    sql += ' VALUES(%s, %s, %s)'
    values = [index_value, target_id, datetime.now()]
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, tuple(_to_sql_value(v) for v in values))
    shard_conn.commit()

def _kv_shard_insert_edge(shard_conn, edge_id, from_id, to_id, data):
    now = datetime.now()
    sql = 'INSERT into kvetch_edges (edge_id, from_id, to_id, body, created, updated) '
    sql += 'VALUES(%s, %s, %s, %s, %s, %s)'
    values = (edge_id, from_id.bytes, to_id.bytes, data_to_body(data), now, now)
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, values)
    shard_conn.commit()

def _kv_shard_get_edges(shard_conn, edge_id, from_id, after, first):
    sql = 'SELECT from_id, to_id, created, body '
    sql += 'FROM kvetch_edges WHERE edge_id = %s AND from_id = %s'
    args = [edge_id, from_id.bytes]
    if after:
        sql += """AND row_id >
        (SELECT row_id from kvetch_edges WHERE edge_id = %s
        AND from_id = %s
        AND to_id = %s) """
        args.extend([edge_id, from_id.bytes, after.bytes])
    sql += ' ORDER BY row_id'
    if first:
        sql += ' LIMIT %s' % first

    with shard_conn.cursor() as cursor:
        cursor.execute(sql, tuple(args))
        rows = cursor.fetchall()

    edges = []
    for row in rows:
        edges.append({
            'from_id': UUID(bytes=row['from_id']),
            'to_id': UUID(bytes=row['to_id']),
            'created': row['created'],
            'data': body_to_data(row['body']),
        })

    return edges

def _kv_shard_get_index_entries(shard_conn, index_name, index_column, index_value, target_column):
    sql = 'SELECT %s FROM %s WHERE %s = ' % (target_column, index_name, index_column)
    sql += '%s'
    sql += ' ORDER BY %s' % target_column
    # print(sql)
    # print(type(index_value))
    rows = []
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, (_to_sql_value(index_value)))
        rows = cursor.fetchall()
        # print('rows')
        # print(rows)

    for row in rows:
        row['target_id'] = UUID(bytes=row['target_id'])
    return rows
