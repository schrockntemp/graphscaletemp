from graphscale.utils import execute_coro, param_check
from graphscale.kvetch.kvetch import KvetchShard

def sync_kv_delete_object(shard, obj_id):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_delete_object(obj_id))

def sync_kv_update_object(shard, obj_id, data):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_update_object(obj_id, data))

def sync_kv_insert_object(shard, new_id, type_id, data):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_insert_object(new_id, type_id, data))

def sync_kv_get_object(shard, id_):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_object(id_))

def sync_kv_get_objects(shard, ids):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_objects(ids))

def sync_kv_insert_index_entry(shard, index_name, index_value, target_id):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_insert_index_entry(index_name, index_value, target_id))

def sync_kv_insert_edge(shard, edge_def, from_id, to_id, data=None):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_insert_edge(edge_def, from_id, to_id, data))

def sync_kv_get_index_entries(shard, index, index_value):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_index_entries(index, index_value))

def sync_kv_get_index_ids(shard, index, index_value):
    param_check(shard, KvetchShard, 'shard')
    entries = sync_kv_get_index_entries(shard, index, index_value)
    return [entry['target_id'] for entry in entries]

def sync_kv_get_edge_ids(shard, edge_def, from_id, after=None, first=None):
    param_check(shard, KvetchShard, 'shard')
    return execute_coro(shard.gen_edge_ids(edge_def, from_id, after, first))
