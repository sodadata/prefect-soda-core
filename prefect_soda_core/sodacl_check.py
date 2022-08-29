"""SodaCL check block"""
from typing import Optional

from prefect.blocks.core import Block
from pydantic import HttpUrl, root_validator

from prefect_soda_core.exceptions import SodaConfigurationException


class SodaCLCheck(Block):
    """
    This block represents a SodaCL check that can be used when running Soda scans.
    """

    sodacl_yaml_file: Optional[str]
    sodacl_yaml_files: Optional[str]
    sodacl_yaml_str: Optional[str]

    _block_type_name: Optional[str] = "SodaCL Check"
    _logo_url: Optional[HttpUrl] = "https://www.to.do"  # noqa

    @root_validator(pre=True)
    def check_block_configuration(cls, values):
        """
        Ensure that at least one configuration option is passed
        """
        sodacl_yaml_file_exists = bool(values.get("sodacl_yaml_file"))
        sodacl_yaml_files_exists = bool(values.get("sodacl_yaml_files"))
        sodacl_yaml_str_exists = bool(values.get("sodacl_yaml_str"))

        if not (
            sodacl_yaml_file_exists
            or sodacl_yaml_files_exists
            or sodacl_yaml_str_exists
        ):
            msg = "Please provide the path to either a checks file, a checks folder, or a checks YAML string."  # noqa
            raise SodaConfigurationException(msg)

        return values
