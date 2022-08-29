from prefect_soda_core.sodacl_check import SodaCLCheck


def test_sodacl_check_construction():
    soda_check_file_path = "/path/to/check_file.yaml"
    scl = SodaCLCheck(sodacl_yaml_files=soda_check_file_path)

    assert scl.sodacl_yaml_files == soda_check_file_path
