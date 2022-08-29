"""Soda configuration block"""
from typing import Optional

from prefect.blocks.core import Block
from pydantic import HttpUrl, root_validator

from prefect_soda_core.exceptions import SodaConfigurationException


class SodaConfiguration(Block):
    """
    This block can be used to configuration required
    to run Soda scans.
    For more information, please refer to the
    [official docs](https://docs.soda.io/soda-core/configuration.html#configuration-instructions)  # noqa
    """

    configuration_yaml_file: Optional[str]
    configuration_yaml_env_var: Optional[str]
    configuration_yaml_env_vars: Optional[str]
    configuration_yaml_str: Optional[str]

    _block_type_name: Optional[str] = "Soda Configuration"
    _logo_url: Optional[HttpUrl] = "https://www.TODO.todo"  # noqa

    @root_validator(pre=True)
    def check_configuration(cls, values):
        """
        Ensure that at least one configuration option is passed
        """
        configuration_yaml_file_exists = bool(values.get("configuration_yaml_file"))
        configuration_yaml_env_var_exists = bool(
            values.get("configuration_yaml_env_var")
        )
        configuration_yaml_env_vars_exists = bool(
            values.get("configuration_yaml_env_vars")
        )
        configuration_yaml_str_exists = bool(values.get("configuration_yaml_str"))

        if not (
            configuration_yaml_file_exists
            or configuration_yaml_env_var_exists
            or configuration_yaml_env_vars_exists
            or configuration_yaml_str_exists
        ):
            msg = "Please provide at least one Soda configuration option."
            raise SodaConfigurationException(msg)

        return values
