from unittest import mock

import pytest
from prefect import flow

from prefect_soda_core.soda_configuration import SodaConfiguration
from prefect_soda_core.sodacl_check import SodaCLCheck
from prefect_soda_core.tasks import soda_scan_execute


def _mock_shell_run_command_fn(**kwargs):
    msg = "this is the log"
    return msg.split(" ")


@mock.patch("prefect_soda_core.tasks.shell_run_command.fn")
def test_soda_scan_execute_raises(mock_shell_run_command_fn):
    mock_shell_run_command_fn.return_value = _mock_shell_run_command_fn()
    mock_shell_run_command_fn.side_effect = RuntimeError("error!")

    @flow(name="soda_scan_execute_raises")
    def test_flow():
        result = soda_scan_execute(
            data_source_name="test",
            configuration=SodaConfiguration(
                configuration_yaml_path="/path/to/config.yaml",
                configuration_yaml_str=None,
            ),
            checks=SodaCLCheck(
                sodacl_yaml_path="/path/to/checks.yaml", sodacl_yaml_str=None
            ),
            variables=None,
        )
        return result

    with pytest.raises(RuntimeError, match="error!"):
        test_flow()

    mock_shell_run_command_fn.assert_called_once()


@mock.patch("prefect_soda_core.tasks.shell_run_command.fn")
def test_soda_scan_execute_succeed(mock_shell_run_command_fn):
    mock_shell_run_command_fn.return_value = _mock_shell_run_command_fn()

    @flow(name="soda_scan_execute_succeed")
    def test_flow():
        result = soda_scan_execute(
            data_source_name="test",
            configuration=SodaConfiguration(
                configuration_yaml_path="/path/to/config.yaml",
                configuration_yaml_str=None,
            ),
            checks=SodaCLCheck(
                sodacl_yaml_path="/path/to/checks.yaml", sodacl_yaml_str=None
            ),
            variables={"foo": "bar"},
            verbose=True,
        )
        return result

    flow_result = test_flow()

    assert flow_result == "this is the log".split(" ")
