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
    configuration_yaml_files: Optional[str]
    configuration_yaml_str: Optional[str]

    _block_type_name: Optional[str] = "Soda Configuration"
    _logo_url: Optional[HttpUrl] = "https://www.TODO.todo"  # noqa

    @root_validator(pre=True)
    def check_block_configuration(cls, values):
        """
        Ensure that at least one configuration option is passed
        """
        configuration_yaml_file_exists = bool(values.get("configuration_yaml_file"))
        configuration_yaml_files_exists = bool(values.get("configuration_yaml_files"))
        configuration_yaml_str_exists = bool(values.get("configuration_yaml_str"))

        if not (
            configuration_yaml_file_exists
            or configuration_yaml_files_exists
            or configuration_yaml_str_exists
        ):
            msg = "Please provide the path to either a configuration file, a configuration folder, or a configuration YAML string."  # noqa
            raise SodaConfigurationException(msg)

        return values
