import redis

def test_redis():
    redis_instance = redis.StrictRedis(host='localhost', port=6379, db=0, decode_responses=True)
    redis_instance.set('foo', 'bar')
    assert redis_instance.get('foo') == 'bar'
