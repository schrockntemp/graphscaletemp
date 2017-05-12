from graphscale.kvetch import Kvetch
from graphscale.kvetch.kvetch_dbshard import (
    KvetchDbShard,
    KvetchDbSingleConnectionPool,
    KvetchDbEdgeDefinition,
    KvetchDbIndexDefinition,
)

from graphscale.kvetch.kvetch_dbschema import (
    init_shard_db_tables,
    drop_shard_db_tables,
)

def get_indexes():
    provider_index = KvetchDbIndexDefinition(
        indexed_attr='provider',
        indexed_type_id=100000, # Provider
        sql_type_of_index='CHAR(255)',
        index_name='Provider_provider_to_Provider_obj_id_index',
    )
    return [provider_index]

def get_edges():
    provider_to_report = KvetchDbEdgeDefinition(
        edge_name='provider_to_report',
        edge_id=93843948,
        from_id_attr='provider_id',
    )
    return [provider_to_report]

def init_hcris_db_kvetch(conn):
    shards = [KvetchDbShard(pool=KvetchDbSingleConnectionPool(conn))]
    indexes = get_indexes()
    drop_shard_db_tables(shards[0], indexes)
    init_shard_db_tables(shards[0], indexes)
    return Kvetch(shards=shards, edges=get_edges(), indexes=indexes)

def create_hcris_db_kvetch(conn):
    shards = [KvetchDbShard(pool=KvetchDbSingleConnectionPool(conn))]
    indexes = get_indexes()

    init_shard_db_tables(shards[0], indexes)
    return Kvetch(shards=shards, edges=get_edges(), indexes=indexes)
