from graphscale.utils import execute_sql, param_check

def create_kvetch_objects_table_sql():
    return """CREATE TABLE IF NOT EXISTS kvetch_objects (
    row_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    id BINARY(16) NOT NULL,
    type_id INT NOT NULL,
    created DATETIME NOT NULL,
    updated DATETIME NOT NULL,
    body MEDIUMBLOB,
    UNIQUE KEY (id),
    KEY (updated)
) ENGINE=InnoDB;
"""

def create_kvetch_index_table_sql(index_column, index_sql_type, target_column, index_name):
    param_check(index_column, str, 'index_column')
    param_check(target_column, str, 'target_column')
    param_check(index_name, str, 'index_name')

    # something is up here. the two indexing keys (not updated) should be unique
    return """CREATE TABLE %s (
    row_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    %s %s NOT NULL,
    %s BINARY(16) NOT NULL,
    created DATETIME NOT NULL,
    KEY (%s, %s),
    KEY (%s, %s),
    KEY (created)
) ENGINE=InnoDB;
""" % (index_name, index_column, index_sql_type, target_column,
       index_column, target_column, target_column, index_column)

def create_kvetch_edge_table_sql():
    return """CREATE TABLE IF NOT EXISTS kvetch_edges (
    row_id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    edge_id INT NOT NULL,
    from_id BINARY(16) NOT NULL,
    to_id BINARY(16) NOT NULL,
    created DATETIME NOT NULL,
    updated DATETIME NOT NULL,
    body MEDIUMBLOB,
    UNIQUE KEY(edge_id, from_id, to_id),
    UNIQUE KEY(edge_id, from_id, row_id),
    KEY(updated)
) ENGINE=InnoDB;
"""

def create_kvetch_objects_table(shard):
    execute_sql(shard.conn(), create_kvetch_objects_table_sql())

def create_kvetch_edges_table(shard):
    execute_sql(shard.conn(), create_kvetch_edge_table_sql())

def create_kvetch_index_table(shard, shard_index):
    sql = create_kvetch_index_table_sql(
        shard_index.indexed_attr(),
        shard_index.indexed_sql_type(),
        'target_id',
        shard_index.index_name())
    execute_sql(shard.conn(), sql)

def init_shard_db_tables(shard, indexes):
    create_kvetch_objects_table(shard)
    create_kvetch_edges_table(shard)
    for shard_index in indexes.values():
        create_kvetch_index_table(shard, shard_index)

def drop_shard_db_tables(shard, indexes):
    execute_sql(shard.conn(), 'DROP TABLE IF EXISTS kvetch_objects')
    execute_sql(shard.conn(), 'DROP TABLE IF EXISTS kvetch_edges')
    for shard_index in indexes.values():
        execute_sql(shard.conn(), 'DROP TABLE IF EXISTS %s' % shard_index.index_name())
