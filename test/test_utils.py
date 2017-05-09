import pymysql
import pymysql.cursors

from graphscale.kvetch.kvetch_dbschema import (
    create_kvetch_index_table_sql,
    create_kvetch_objects_table_sql
)

def create_kvetch_index_table(conn, index_name):
    with conn.cursor() as cursor:
        cursor.execute(create_kvetch_index_table_sql(index_name))
    conn.commit()

def create_kvetch_objects_table(conn):
    with conn.cursor() as cursor:
        cursor.execute(create_kvetch_objects_table_sql())
    conn.commit()

def drop_objects_table(conn):
    with conn.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS kvetch_objects')
    conn.commit()

def drop_index_table(conn, index_name):
    with conn.cursor() as cursor:
        cursor.execute('DROP TABLE IF EXISTS kvetch_%s_edges' % index_name)
    conn.commit()

class MagnusConn:
    conns = {}

    @staticmethod
    def get_unittest_conn():
        return MagnusConn.get_conn('graphscale-unittest')
        # if MagnusConn.conn is not None:
        #     return MagnusConn.conn
        # MagnusConn.conn = pymysql.connect(
        #     host='localhost',
        #     user='magnus',
        #     password='magnus',
        #     db='unittest_mysql_db',
        #     charset='utf8mb4',
        #     cursorclass=pymysql.cursors.DictCursor)
        # return MagnusConn.conn

    @staticmethod
    def get_conn(db_name):
        if db_name in MagnusConn.conns:
            return MagnusConn.conns[db_name]
        MagnusConn.conns[db_name] = pymysql.connect(
            host='localhost',
            user='magnus',
            password='magnus',
            db=db_name,
            # was utf8mb4
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        return MagnusConn.conns[db_name]

