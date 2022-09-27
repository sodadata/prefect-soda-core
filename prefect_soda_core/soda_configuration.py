"""Soda configuration block"""
from typing import Optional

from prefect.blocks.core import Block
from pydantic import HttpUrl, root_validator
from yaml import safe_dump, safe_load
from yaml.error import YAMLError

from prefect_soda_core.exceptions import SodaConfigurationException


class SodaConfiguration(Block):
    """
    This block can be used to provide the configuration
    required to run Soda scans.
    For more information, please refer to the
    [official docs](https://docs.soda.io/soda-core/configuration.html#configuration-instructions)  # noqa

    Args:
        configuration_yaml_path (str): Absolute path of the Soda configuration file.
        configuration_yaml_str (str): Optional YAML string containing the Soda configuration
            details. If provided, it will be saved
            at the path provided with `configuration_yaml_path`.

    Example:
        Load stored Soda configuration.
        ```python
        from prefect_soda_core.soda_configuration import SodaConfiguration
        soda_configuration_block = SodaConfiguration.load("BLOCK_NAME")
        ```
    """

    configuration_yaml_path: str
    configuration_yaml_str: Optional[str]

    _block_type_name: Optional[str] = "Soda Configuration"
    _logo_url: Optional[HttpUrl] = "https://www.TODO.todo"  # noqa

    @root_validator(pre=True)
    def check_block_configuration(cls, values):
        """
        Ensure that the configuration options are valid.
        A configuration is valid if it provides just the path to the
        YAML configuration file or if it has both the path
        to the configuration file and a valid YAML configuration string.

        Raises:
            SodaConfigurationException: When the provided configuration is not valid.
        """
        configuration_yaml_str_exists = bool(values.get("configuration_yaml_str"))

        # If the YAML string is passed, but is not a valid YAML, then raise error
        if configuration_yaml_str_exists:
            try:
                yaml_str = values.get("configuration_yaml_str")
                safe_load(yaml_str)
            except YAMLError as exc:
                msg = f"The provided configuration YAML is not valid. Error is: {exc}"
                raise SodaConfigurationException(msg)

        return values

    def persist_configuration(self):
        """
        Persist Soda configuration on the file system, if necessary.
        Please note that, if the path already exists, it will be overwritten.
        """

        # If a YAML string and path are passed, then persist the configuration
        if self.configuration_yaml_str and self.configuration_yaml_path:
            with open(self.configuration_yaml_path, "w") as f:
                safe_dump(data=self.configuration_yaml_str, stream=f)
