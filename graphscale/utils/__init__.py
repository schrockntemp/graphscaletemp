import asyncio

def param_check(obj, ttype, param):
    if not isinstance(obj, ttype):
        if obj == id:
            raise ValueError("obj is id. typo?")
        raise ValueError('Param %s is not a %s. It is a %s. Value: %s'
                         % (param, ttype.__name__, type(param).__name__, str(obj)))

def execute_gen(gen):
    loop = asyncio.new_event_loop()
    result = loop.run_until_complete(gen)
    loop.close()
    return result

def execute_sql(conn, sql):
    with conn.cursor() as cursor:
        cursor.execute(sql)
    conn.commit()
    return sql

async def async_array(coros):
    return await asyncio.gather(*coros)

async def async_tuple(*coros):
    return tuple(await asyncio.gather(*coros))
