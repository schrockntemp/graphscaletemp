#import pymysql
#import pymysql.cursors;
#import pytest
#from kvetch_dbschema import create_kvetch_edges_table_sql
#
#
#
#if __name__ == "__main__":
#    conn = pymysql.connect(host='localhost',
#                           user='magnus',
#                           password='magnus',
#                           db='unittest_mysql_db',
#                           charset='utf8mb4',
#                           cursorclass=pymysql.cursors.DictCursor)
#    sql = create_kvetch_edges_table_sql('test')
#    print(sql)
#    with conn.cursor() as cursor:
#        cursor.execute(sql)
#    conn.commit()
#
