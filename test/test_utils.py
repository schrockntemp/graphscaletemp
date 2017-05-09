import pymysql
import pymysql.cursors

class MagnusConn:
    conns = {}

    @staticmethod
    def get_unittest_conn():
        return MagnusConn.get_conn('graphscale-unittest')

    @staticmethod
    def is_db_unittest_up():
        """Tests to see if the unittest-mysql is up and running on the localhost
        This allows for the conditional execution of tests on the db shards in addition
        to the memory shards"""
        try:
            MagnusConn.get_unittest_conn()
        except pymysql.err.OperationalError:
            return False
        return True

    @staticmethod
    def get_conn(db_name):
        if db_name in MagnusConn.conns:
            return MagnusConn.conns[db_name]
        MagnusConn.conns[db_name] = pymysql.connect(
            host='localhost',
            user='magnus',
            password='magnus',
            db=db_name,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor)
        return MagnusConn.conns[db_name]

def db_mem_fixture(*, mem, db):
    fixture_funcs = []
    if MagnusConn.is_db_unittest_up():
        fixture_funcs.append(db)
    fixture_funcs.append(mem)
    return fixture_funcs
