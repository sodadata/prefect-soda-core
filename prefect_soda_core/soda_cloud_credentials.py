"""Soda Cloud credentials block"""
from typing import Dict, Optional

from prefect.blocks.core import Block
from pydantic import HttpUrl, SecretStr
from yaml import safe_dump


class SodaCloudCredentials(Block):
    """
    This block can be used to manage authentication information
    that are needed to authenticate with Soda Cloud.
    """

    host: Optional[str] = "cloud.soda.io"
    api_key_id: SecretStr
    api_key_secret: SecretStr

    _block_type_name: Optional[str] = "Soda Cloud Credentials"
    _logo_url: Optional[HttpUrl] = "https://www.TODO.todo"  # noqa

    def get_block_as_json(self) -> Dict:
        """
        Returns the block JSON representation.
        Returns:
            JSON representation of the block.
        """
        return {
            "soda_cloud": {
                "host": self.host,
                "api_key_id": self.api_key_id.get_secret_value(),
                "api_key_secret": self.api_key_secret.get_secret_value(),
            }
        }

    def get_block_as_yaml_str(self) -> str:
        """
        Returns the block YAML string representation.
        Returns:
            YAML string representation of the block.
        """
        return safe_dump(data=self.get_block_as_json())
