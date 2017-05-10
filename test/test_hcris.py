import asyncio
import pytest
from uuid import UUID

from graphql import graphql
from graphql.execution.executors.asyncio import AsyncioExecutor

from graphscale.examples.hcris.hcris_graphql import create_hcris_schema
from graphscale.examples.hcris.hcris_pent import (
    get_hcris_config,
    create_hospital,
    CreateHospitalInput,
)

from graphscale.kvetch import Kvetch
from graphscale.kvetch.kvetch_memshard import KvetchMemShard

from graphscale.pent import PentContext

from graphscale.utils import execute_gen

def execute_query(query, pent_context, graphql_schema):
    loop = asyncio.new_event_loop()
    result = graphql(
        graphql_schema,
        query,
        executor=AsyncioExecutor(loop=loop),
        context_value=pent_context
    )
    if result.errors:
        print(result.errors[0])
        raise Exception(str(result.errors))
    return result

def mem_context():
    shard = KvetchMemShard()
    kvetch = Kvetch(shards=[shard], edges=[], indexes=[])
    return PentContext(kvetch=kvetch, config=get_hcris_config())

def test_insert_get():
    pent_context = mem_context()

    hospital_input = CreateHospitalInput(provider='102020')
    hospital = execute_gen(create_hospital(pent_context, hospital_input))
    assert isinstance(hospital.obj_id(), UUID)
    assert hospital.provider_number() == '102020'

    graphql_schema = create_hcris_schema()
    hospital_id = hospital.obj_id()
    query = '{ hospital(id: "%s") { providerNumber } }' % hospital_id
    result = execute_query(query, pent_context, graphql_schema)
    assert result.data['hospital']['providerNumber'] == '102020'
