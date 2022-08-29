"""SodaCL check block"""
from typing import Optional

from prefect.blocks.core import Block
from pydantic import HttpUrl


class SodaCLCheck(Block):
    """
    This block represents a SodaCL check that can be used when running Soda scans.
    """

    sodacl_yaml_files: str

    _block_type_name: Optional[str] = "SodaCL Check"
    _logo_url: Optional[HttpUrl] = "https://www.to.do"  # noqa
