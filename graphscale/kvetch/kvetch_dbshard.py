import pickle
import zlib
from datetime import datetime
from uuid import UUID, uuid4

from pymysql.connections import Connection

from graphscale.utils import (
    execute_coro,
    param_check,
    async_array
)

from graphscale.kvetch.kvetch import (
    KvetchShard,
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

def _kv_shard_insert_object(shard_conn, type_id, data):
    with shard_conn.cursor() as cursor:
        new_id = uuid4()
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
        target_value):

    sql = 'INSERT INTO %s (%s, %s, updated)' % (index_name, index_column, target_column)
    sql += ' VALUES(%s, %s, %s)'
    values = [index_value, target_value, datetime.now()]
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, tuple(_to_sql_value(v) for v in values))
    shard_conn.commit()

def _kv_shard_get_index_entries(shard_conn, index_name, index_column, index_value, target_column):
    sql = 'SELECT %s FROM %s WHERE %s =' % (target_column, index_name, index_column)
    sql += '%s'
    sql += ' ORDER BY %s' % target_column
    with shard_conn.cursor() as cursor:
        cursor.execute(sql, (_to_sql_value(index_value)))
        rows = cursor.fetchall()
    return rows

class Kvetch:
    def __init__(self, shards):
        self._shards = shards

    def get_shard(self, id_):
        shard_id = self._shard_id_of_id(id_)
        return self._shards[shard_id]

    def _shard_id_of_id(self, id_):
        # do something less stupid like consistent hashing
        # excellent description here http://michaelnielsen.org/blog/consistent-hashing/
        return int(id_) % len(self._shards)

    async def gen_insert_object(self, type_id, data):
        new_id = uuid4()
        return await self.get_shard(new_id).gen_insert_object(type_id, data)

    async def gen_object(self, id_):
        return await self.get_shard(id_).gen_object(id_)

    # not tested yet
    async def gen_objects(self, ids):
        shard_to_ids = {} # shard_id => [id]
        for id_ in ids:
            shard_id = self._shard_id_of_id(id_)
            if not shard_id in shard_to_ids:
                shard_to_ids[shard_id] = []
            shard_to_ids[shard_id].append(id_)

        coros = []
        for shard_id, ids_in_shard in shard_to_ids.items():
            shard = self._shards[shard_id]
            coros.append(shard.gen_objects(ids_in_shard))

        results = {}
        obj_dict_per_shard = await async_array(coros)
        for obj_dict in obj_dict_per_shard:
            for id_, obj in obj_dict.items():
                results[id_] = obj
        return results

class KvetchDbSingleConnectionPool:
    def __init__(self, conn):
        param_check(conn, Connection, 'conn')
        self._conn = conn

    def conn(self):
        return self._conn

def sync_kv_insert_object(shard, type_id, data):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_insert_object(type_id, data))

def sync_kv_get_object(shard, id_):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_object(id_))

def sync_kv_get_objects(shard, ids):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_objects(ids))

def sync_kv_index_get_all(shard, index, value):
    param_check(shard, KvetchShard, 'shard')
    param_check(index, KvetchShardIndex, 'index')
    return execute_coro(index.gen_all(shard, value))

class KvetchShardIndex:
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

    async def gen_all(self, shard, value):
        entries = _kv_shard_get_index_entries(
            shard_conn=shard.conn(),
            index_name=self.index_name(),
            index_column=self.indexed_attr(),
            index_value=value,
            target_column='entity_id',
        )
        ids = [UUID(bytes=entry['entity_id']) for entry in entries]
        objs = await shard.gen_objects(ids)
        return objs

class KvetchDbShard(KvetchShard):
    def __init__(self, *, pool, indexes):
        self._pool = pool
        self._index_dict = dict(zip([index.index_name() for index in indexes], indexes))

    @staticmethod
    def fromconn(conn):
        return KvetchDbShard(pool=KvetchDbSingleConnectionPool(conn), indexes=[])

    def conn(self):
        return self._pool.conn()

    def indexes(self):
        return self._index_dict.values()

    def index_by_name(self, name):
        return self._index_dict[name]

    async def gen_object(self, id_):
        param_check(id_, UUID, 'id_')
        return _kv_shard_get_object(self.conn(), id_)

    async def gen_objects(self, ids):
        param_check(ids, list, 'ids')
        if not ids:
            raise ValueError('ids must have at least 1 element')
        return _kv_shard_get_objects(self.conn(), ids)

    async def gen_insert_object(self, type_id, data):
        param_check(type_id, int, 'type_id')
        param_check(data, dict, 'data')
        if 'id' in data:
            raise ValueError('Cannot specify id')

        if '__type_id' in data:
            raise ValueError('Cannot specify __type_id')
        new_id = _kv_shard_insert_object(self.conn(), type_id, data)

        for index_name, index in self._index_dict.items():
            attr = index.indexed_attr()
            if attr in data and data[attr]:
                _kv_shard_insert_index_entry(
                    shard_conn=self.conn(),
                    index_name=index_name,
                    index_column=attr,
                    index_value=data[attr],
                    target_column='entity_id',
                    target_value=new_id,
                )

        return new_id
