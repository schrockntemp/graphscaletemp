from graphscale.utils import param_check

class KvetchShard:
    def check_insert_object_vars(self, type_id, data):
        param_check(type_id, int, 'type_id')
        param_check(data, dict, 'data')
        if 'id' in data:
            raise ValueError('Cannot specify id')

        if '__type_id' in data:
            raise ValueError('Cannot specify __type_id')

class KvetchShardIndex:
    async def gen_all(self, shard, value):
        raise Exception('must implement')

