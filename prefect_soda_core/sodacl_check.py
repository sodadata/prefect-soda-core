"""SodaCL check block"""
from typing import Optional

from prefect.blocks.core import Block
from pydantic import HttpUrl, root_validator
from yaml import safe_dump, safe_load
from yaml.error import YAMLError

from prefect_soda_core.exceptions import SodaConfigurationException


class SodaCLCheck(Block):
    """
    This block represents a SodaCL check that can be used when running Soda scans.

    Args:
        sodacl_yaml_path (str): Absolute path of the Soda Checks file.
        sodacl_yaml_str (str): Optional YAML string containing the Soda Checks
            details. If provided, it will be saved
            at the path provided with `sodacl_yaml_path`.

    Example:
        ```python
        from prefect_soda_core.sodacl_check import SodaCLCheck
        sodacl_check_block = SodaCLCheck.load("BLOCK_NAME")
        ```
    """

    sodacl_yaml_path: str
    sodacl_yaml_str: Optional[str]

    _block_type_name: Optional[str] = "SodaCL Check"
    _logo_url: Optional[HttpUrl] = "https://www.to.do"  # noqa

    @root_validator(pre=True)
    def check_block_configuration(cls, values):
        """
        Ensure that the check configuration options are valid.
        A check configuration is valid if it provides just the path to the
        YAML Soda checks file or if it has both the path
        to the Soda checks file and a valid YAML Soda checks string.

        Raises:
            SodaConfigurationException: When the provided checks configuration
                is not valid.
        """
        sodacl_yaml_str_exists = bool(values.get("sodacl_yaml_str"))

        # If the YAML string is passed, but is not a valid YAML, then raise error
        if sodacl_yaml_str_exists:
            try:
                yaml_str = values.get("sodacl_yaml_str")
                safe_load(yaml_str)
            except YAMLError as exc:
                msg = f"The provided checks YAML is not valid. Error is: {exc}"
                raise SodaConfigurationException(msg)

        return values

    def persist_checks(self):
        """
        Persist Soda checks on the file system, if necessary.
        Please note that, if the path already exists, it will be overwritten.
        """

        # If a YAML string and path are passed, then persist the configuration
        if self.sodacl_yaml_str and self.sodacl_yaml_path:
            with open(self.sodacl_yaml_path, "w") as f:
                safe_dump(data=self.sodacl_yaml_str, stream=f)
