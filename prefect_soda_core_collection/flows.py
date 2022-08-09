"""This is an example flows module"""
from prefect import flow

from prefect_soda_core_collection.tasks import (
    goodbye_prefect_soda_core_collection,
    hello_prefect_soda_core_collection,
)


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_soda_core_collection)
    print(goodbye_prefect_soda_core_collection)
    return "Done"
