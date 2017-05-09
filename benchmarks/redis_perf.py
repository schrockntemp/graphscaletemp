import redis
from uuid import uuid4

from timeit import default_timer as timer

from graphscale.kvetch.kvetch_utils import data_to_body, body_to_data
from graphscale.utils import execute_gen
from test.test_utils import MagnusConn
from graphscale.kvetch.kvetch_dbshard import (
        KvetchDbShard,
        KvetchDbSingleConnectionPool,
)

from graphscale.kvetch.kvetch_dbschema import (
    drop_shard_db_tables,
    init_shard_db_tables,
)

redis_instance = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=False)


def time_set(reps):
    start = timer()

    for i in range(0,reps):
        data = {'num': i, 'str': 'some string' + str(i)}
        body = data_to_body(data)
        redis_instance.set(i, body)
    
    end = timer()
    
    set_microseconds = (end - start) * 1000 * 1000
    
    print('%s microseconds per set ' % (set_microseconds/reps))

def time_get(reps):

    start = timer()

    for i in range(0,reps):
        body = redis_instance.get(i)
        data = body_to_data(body)
    
    end = timer()
    
    get_microseconds = (end - start) * 1000 * 1000
    
    print('%s microseconds per get' % (get_microseconds/reps))


def time_mget(reps):
    start = timer()
    
    bodies = redis_instance.mget(range(0,reps))
    datas = [body_to_data(body) for body in bodies]
    
    end = timer()
    
    mget_microseconds = (end - start) * 1000 * 1000
    
    print('%s microseconds per obj in mget' % (mget_microseconds/reps))

async def gen_time_insert_things(reps):
    conn = MagnusConn.get_unittest_conn()
    shard = KvetchDbShard(
        pool=KvetchDbSingleConnectionPool(conn),
    )
    drop_shard_db_tables(shard, {}) 
    init_shard_db_tables(shard, {}) 
    start = timer()
    ids = []
    for i in range(0, reps):
        new_id = uuid4()
        ids.append(new_id)
        data = {'num': i, 'str': 'some string' + str(i)}
        await shard.gen_insert_object(new_id, 1000, data)
    end = timer()
    db_insert_microseconds = (end - start) * 1000 * 1000
    print('%s microseconds per db insert' % (db_insert_microseconds/reps))
    return ids

async def gen_time_get_things(ids):
    conn = MagnusConn.get_unittest_conn()
    shard = KvetchDbShard(
        pool=KvetchDbSingleConnectionPool(conn),
    )
    start = timer()
    for obj_id in ids:
        obj = await shard.gen_object(obj_id)
    end = timer()
    
    reps = len(ids)
    db_get_microseconds = (end - start) * 1000 * 1000
    print('%s microseconds per db get' % (db_get_microseconds/reps))

time_set(1000)
time_get(1000)
time_mget(1000)

ids = execute_gen(gen_time_insert_things(100))
execute_gen(gen_time_get_things(ids))
