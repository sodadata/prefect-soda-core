import pytest

from prefect_soda_core.exceptions import SodaConfigurationException
from prefect_soda_core.soda_configuration import SodaConfiguration


def test_soda_configuration_construction_raises():
    msg_match = "Please provide at least one Soda configuration option."
    with pytest.raises(SodaConfigurationException, match=msg_match):
        SodaConfiguration(
            configuration_yaml_file=None,
            configuration_yaml_env_var=None,
            configuration_yaml_env_vars=None,
            configuration_yaml_str=None,
        )


def test_soda_configuration_construction_with_yaml_file_succeed():
    yaml_file_path = "/path/to/configuration.yaml"

    scc = SodaConfiguration(
        configuration_yaml_file=yaml_file_path,
        configuration_yaml_env_var=None,
        configuration_yaml_env_vars=None,
        configuration_yaml_str=None,
    )

    assert scc.configuration_yaml_file == yaml_file_path
