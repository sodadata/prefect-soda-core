from typing import Dict
from unittest import mock

import pytest
from prefect import flow

from prefect_soda_core.exceptions import SodaScanRunException
from prefect_soda_core.soda_configuration import SodaConfiguration
from prefect_soda_core.sodacl_check import SodaCLCheck
from prefect_soda_core.tasks import soda_scan_execute


class ScanMock:
    def add_configuration_yaml_file(file_path: str):
        pass

    def add_configuration_yaml_files(
        path: str, recursive: bool = True, suffixes: str = None
    ):
        pass

    def add_configuration_yaml_str(
        environment_yaml_str: str, file_path: str = "yaml string"
    ):
        pass

    def add_sodacl_yaml_file(file_path: str):
        pass

    def add_sodacl_yaml_files(path: str, recursive: bool = True, suffixes: str = None):
        pass

    def add_sodacl_yaml_str(sodacl_yaml_str: str):
        pass

    def add_variables(variables: Dict[str, str]):
        pass

    def set_data_source_name(data_source_name: str):
        pass

    def set_verbose(verbose_var: bool):
        pass

    def disable_telemetry():
        pass


@mock.patch("prefect_soda_core.tasks.Scan")
def test_soda_scan_execute_raises(scan_mock):
    def get_error_logs_text() -> str:
        return "error!"

    def execute():
        return 3

    scan_mock.return_value = ScanMock
    scan_mock.return_value.execute = execute
    scan_mock.return_value.get_error_logs_text = get_error_logs_text

    @flow(name="soda_scan_execute_raises")
    def test_flow():
        return soda_scan_execute(
            data_source_name="foo",
            configuration=SodaConfiguration(
                configuration_yaml_file="/path/to/configuration_file.yaml",
                configuration_yaml_files=None,
                configuration_yaml_str=None,
            ),
            checks=SodaCLCheck(
                sodacl_yaml_file="/path/to/checks_file.yaml",
                sodacl_yaml_files=None,
                sodacl_yaml_str=None,
            ),
            variables={"foo": "bar"},
            disable_telemetry=True,
        )

    with pytest.raises(
        SodaScanRunException, match="Soda scan encountered an error: error!"
    ):
        test_flow()


@mock.patch("prefect_soda_core.tasks.Scan")
def test_soda_scan_execute_succeed_with_configuration_yaml_file_and_checks_yaml_file(
    scan_mock,
):
    def get_all_checks_text():
        return "Succeed!"

    def execute():
        pass

    scan_mock.return_value = ScanMock
    scan_mock.return_value.execute = execute
    scan_mock.return_value.get_all_checks_text = get_all_checks_text

    @flow(name="soda_scan_execute_succeed")
    def test_flow():
        return soda_scan_execute(
            data_source_name="foo",
            configuration=SodaConfiguration(
                configuration_yaml_file="/path/to/configuration_file.yaml",
                configuration_yaml_files=None,
                configuration_yaml_str=None,
            ),
            checks=SodaCLCheck(
                sodacl_yaml_file="/path/to/checks_file.yaml",
                sodacl_yaml_files=None,
                sodacl_yaml_str=None,
            ),
            variables={"foo": "bar"},
            disable_telemetry=True,
        )

    result = test_flow()

    assert result.get_all_checks_text() == "Succeed!"


@mock.patch("prefect_soda_core.tasks.Scan")
def test_soda_scan_execute_succeed_with_configuration_yaml_files_and_checks_yaml_files(
    scan_mock,
):
    def get_all_checks_text():
        return "Succeed!"

    def execute():
        pass

    scan_mock.return_value = ScanMock
    scan_mock.return_value.execute = execute
    scan_mock.return_value.get_all_checks_text = get_all_checks_text

    @flow(
        name="soda_scan_execute_succeed_with_configuration_yaml_files_and_checks_yaml_files"  # noqa
    )
    def test_flow():
        return soda_scan_execute(
            data_source_name="foo",
            configuration=SodaConfiguration(
                configuration_yaml_file=None,
                configuration_yaml_files="/path/to/configuration_folder/",
                configuration_yaml_str=None,
            ),
            checks=SodaCLCheck(
                sodacl_yaml_file=None,
                sodacl_yaml_files="/path/to/checks_folder/",
                sodacl_yaml_str=None,
            ),
            variables={"foo": "bar"},
            disable_telemetry=True,
        )

    result = test_flow()

    assert result.get_all_checks_text() == "Succeed!"


@mock.patch("prefect_soda_core.tasks.Scan")
def test_soda_scan_execute_succeed_with_configuration_yaml_str_and_checks_yaml_str(
    scan_mock,
):
    def get_all_checks_text():
        return "Succeed!"

    def execute():
        pass

    scan_mock.return_value = ScanMock
    scan_mock.return_value.execute = execute
    scan_mock.return_value.get_all_checks_text = get_all_checks_text

    @flow(
        name="soda_scan_execute_succeed_with_configuration_yaml_str_and_checks_yaml_str"
    )
    def test_flow():
        return soda_scan_execute(
            data_source_name="foo",
            configuration=SodaConfiguration(
                configuration_yaml_file=None,
                configuration_yaml_files=None,
                configuration_yaml_str="yaml configuration",
            ),
            checks=SodaCLCheck(
                sodacl_yaml_file=None,
                sodacl_yaml_files=None,
                sodacl_yaml_str="yaml check",
            ),
            variables={"foo": "bar"},
            disable_telemetry=True,
        )

    result = test_flow()

    assert result.get_all_checks_text() == "Succeed!"
