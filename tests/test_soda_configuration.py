import pytest

from prefect_soda_core.exceptions import SodaConfigurationException
from prefect_soda_core.soda_configuration import SodaConfiguration


def test_soda_configuration_construction_with_invalid_yaml_raises():
    msg_match = "The provided configuration YAML is not valid."
    with pytest.raises(SodaConfigurationException, match=msg_match):
        SodaConfiguration(
            configuration_yaml_path="/path/to/configuration.yaml",
            configuration_yaml_str="""
            root:
                - a_list
                a_node
            """,
        )


def test_soda_configuration_construction_without_yaml_succeed():
    expected_path = "/path/to/configuration.yaml"
    sc = SodaConfiguration(
        configuration_yaml_path=expected_path, configuration_yaml_str=None
    )

    assert sc.configuration_yaml_path == expected_path
    assert sc.configuration_yaml_str is None


def test_soda_configuration_construction_with_valid_yaml_succeed():
    expected_path = "/path/to/configuration.yaml"
    expected_yaml = """
    root:
        a_list:
            key1: val1
            key2: val2
    """
    sc = SodaConfiguration(
        configuration_yaml_path=expected_path, configuration_yaml_str=expected_yaml
    )

    assert sc.configuration_yaml_path == expected_path
    assert sc.configuration_yaml_str == expected_yaml
