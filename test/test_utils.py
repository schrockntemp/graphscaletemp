import asyncio
import pymysql
import pymysql.cursors
import traceback

from graphql import graphql
from graphql.execution.executors.asyncio import AsyncioExecutor

from graphscale.utils import param_check, print_error

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
        except pymysql.err.OperationalError as e:
            print('failed')
            print(e)
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


def execute_test_graphql(query, pent_context, graphql_schema):
    param_check(query, str, 'query')
    loop = asyncio.new_event_loop()
    result = graphql(
        graphql_schema,
        query,
        executor=AsyncioExecutor(loop=loop),
        context_value=pent_context
    )
    if result.errors:
        error = result.errors[0]
        print_error('GRAPHQL ERROR')
        print_error(error)
        orig = error.original_error

        print_error('ORIGINAL ERROR')
        print_error(orig)

        trace = orig.__traceback__
        print_error(''.join(traceback.format_tb(trace)))
        raise error
    return result

def exception_stacktrace(error):
    trace = error.__traceback__
    return ''.join(traceback.format_tb(trace))
