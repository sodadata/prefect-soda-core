from prefect import flow

from prefect_soda_core_collection.tasks import (
    goodbye_prefect_soda_core_collection,
    hello_prefect_soda_core_collection,
)


def test_hello_prefect_soda_core_collection():
    @flow
    def test_flow():
        return hello_prefect_soda_core_collection()

    result = test_flow()
    assert result == "Hello, prefect-soda-core-collection!"


def goodbye_hello_prefect_soda_core_collection():
    @flow
    def test_flow():
        return goodbye_prefect_soda_core_collection()

    result = test_flow()
    assert result == "Goodbye, prefect-soda-core-collection!"
