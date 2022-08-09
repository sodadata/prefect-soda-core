from prefect_soda_core_collection.flows import hello_and_goodbye


def test_hello_and_goodbye_flow():
    result = hello_and_goodbye()
    assert result == "Done"
