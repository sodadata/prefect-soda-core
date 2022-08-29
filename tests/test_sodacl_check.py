import pytest

from prefect_soda_core.exceptions import SodaConfigurationException
from prefect_soda_core.sodacl_check import SodaCLCheck


def test_sodacl_check_construction_raises():
    msg_match = "Please provide the path to either a checks file, a checks folder, or a checks YAML string."  # noqa
    with pytest.raises(SodaConfigurationException, match=msg_match):
        SodaCLCheck(sodacl_yaml_file=None, sodacl_yaml_files=None, sodacl_yaml_str=None)


def test_sodacl_check_construction_with_yaml_file_succeed():
    soda_check_file_path = "/path/to/check_file.yaml"
    scl = SodaCLCheck(
        sodacl_yaml_file=soda_check_file_path,
        sodacl_yaml_files=None,
        sodacl_yaml_str=None,
    )

    assert scl.sodacl_yaml_file == soda_check_file_path
