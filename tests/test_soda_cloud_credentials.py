from pydantic import SecretStr
from yaml import dump

from prefect_soda_core.soda_cloud_credentials import SodaCloudCredentials


def test_credentials_construction():
    api_key_id = "foo"
    api_key_secret = "foo"

    scc = SodaCloudCredentials(
        api_key_id=SecretStr(api_key_id), api_key_secret=SecretStr(api_key_secret)
    )

    assert scc.api_key_id.get_secret_value() == api_key_id
    assert scc.api_key_secret.get_secret_value() == api_key_secret
    assert scc.host == "cloud.soda.io"


def test_credentials_get_as_json():
    api_key_id = "foo"
    api_key_secret = "foo"

    scc = SodaCloudCredentials(
        api_key_id=SecretStr(api_key_id), api_key_secret=SecretStr(api_key_secret)
    )

    scc_as_json = scc.get_block_as_json()

    assert scc_as_json == {
        "soda_cloud": {
            "host": "cloud.soda.io",
            "api_key_id": api_key_id,
            "api_key_secret": api_key_secret,
        }
    }


def test_credentials_as_yaml():
    api_key_id = "foo"
    api_key_secret = "foo"

    scc = SodaCloudCredentials(
        api_key_id=SecretStr(api_key_id), api_key_secret=SecretStr(api_key_secret)
    )

    scc_as_yaml = scc.get_block_as_yaml_str()

    scc_as_json = {
        "soda_cloud": {
            "host": "cloud.soda.io",
            "api_key_id": api_key_id,
            "api_key_secret": api_key_secret,
        }
    }

    assert scc_as_yaml == dump(data=scc_as_json)
