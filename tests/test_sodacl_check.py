import pyfakefs  # noqa
import pytest
from yaml import safe_load

from prefect_soda_core.exceptions import SodaConfigurationException
from prefect_soda_core.sodacl_check import SodaCLCheck


def test_sodacl_check_construction_with_invalid_yaml_raises():
    msg_match = "The provided checks YAML is not valid."
    with pytest.raises(SodaConfigurationException, match=msg_match):
        SodaCLCheck(
            sodacl_yaml_path="/path/to/chekcs.yaml",
            sodacl_yaml_str="""
            root:
                - a_list
                a_node
            """,
        )


def test_sodacl_check_construction_without_yaml_succeed():
    expected_path = "/path/to/checks.yaml"
    sc = SodaCLCheck(sodacl_yaml_path=expected_path, sodacl_yaml_str=None)

    assert sc.sodacl_yaml_path == expected_path
    assert sc.sodacl_yaml_str is None


def test_sodacl_check_construction_with_valid_yaml_succeed():
    expected_path = "/path/to/checks.yaml"
    expected_yaml = """
    root:
        a_list:
            key1: val1
            key2: val2
    """
    sc = SodaCLCheck(sodacl_yaml_path=expected_path, sodacl_yaml_str=expected_yaml)

    assert sc.sodacl_yaml_path == expected_path
    assert sc.sodacl_yaml_str == expected_yaml


def test_persist_checks_succeed(fs):
    expected_path = "/path/to/checks.yaml"
    expected_yaml = """
    root:
        a_list:
            key1: val1
            key2: val2
    """
    sc = SodaCLCheck(sodacl_yaml_path=expected_path, sodacl_yaml_str=expected_yaml)

    fs.create_dir("/path/to/")
    sc.persist_checks()

    with open(expected_path, "r") as f:
        persisted_yaml = safe_load(stream=f)

    assert persisted_yaml == expected_yaml
