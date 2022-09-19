"""Soda configuration block"""
from typing import Optional

from prefect.blocks.core import Block
from pydantic import HttpUrl, root_validator
from yaml import safe_dump, safe_load
from yaml.error import YAMLError

from prefect_soda_core.exceptions import SodaConfigurationException


class SodaConfiguration(Block):
    """
    This block can be used to configuration required
    to run Soda scans.
    For more information, please refer to the
    [official docs](https://docs.soda.io/soda-core/configuration.html#configuration-instructions)  # noqa
    """

    configuration_yaml_path: str
    configuration_yaml_str: Optional[str]

    _block_type_name: Optional[str] = "Soda Configuration"
    _logo_url: Optional[HttpUrl] = "https://www.TODO.todo"  # noqa

    @root_validator(pre=True)
    def check_block_configuration(cls, values):
        """
        Ensure that the configuration options are valid
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
        Please note that, if the path already exists, it will be overwritten
        """

        # If a YAML string and path are passed, then persist the configuration
        if self.configuration_yaml_str and self.configuration_yaml_path:
            with open(self.configuration_yaml_path, "w") as f:
                safe_dump(data=self.configuration_yaml_str, stream=f)
