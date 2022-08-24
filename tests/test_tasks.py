from prefect import flow

from prefect_soda_core.tasks import (
    goodbye_prefect_soda_core,
    hello_prefect_soda_core,
)


def test_hello_prefect_soda_core():
    @flow
    def test_flow():
        return hello_prefect_soda_core()

    result = test_flow()
    assert result == "Hello, prefect-soda-core!"


def goodbye_hello_prefect_soda_core():
    @flow
    def test_flow():
        return goodbye_prefect_soda_core()

    result = test_flow()
    assert result == "Goodbye, prefect-soda-core!"
