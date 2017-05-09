import pickle
import zlib

from uuid import UUID

def data_to_body(data):
    return zlib.compress(pickle.dumps(data))

def body_to_data(body):
    if body is None:
        return {}
    return pickle.loads(zlib.decompress((body)))

def row_to_obj(row):
    id_dict = {'id' : UUID(bytes=row['id']), '__type_id' : row['type_id']}
    body_dict = body_to_data(row['body'])
    return {**id_dict, **body_dict}
