"""This is an example flows module"""
from prefect import flow

from prefect_soda_core.tasks import goodbye_prefect_soda_core, hello_prefect_soda_core


@flow
def hello_and_goodbye():
    """
    Sample flow that says hello and goodbye!
    """
    print(hello_prefect_soda_core)
    print(goodbye_prefect_soda_core)
    return "Done"
