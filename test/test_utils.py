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
    conn = None

    @staticmethod
    def get_conn():
        if MagnusConn.conn is not None:
            return MagnusConn.conn
        MagnusConn.conn = pymysql.connect(
            host='localhost',
            user='magnus',
            password='magnus',
            db='unittest_mysql_db',
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        return MagnusConn.conn