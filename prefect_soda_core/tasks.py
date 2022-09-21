"""
Collection of tasks that can be used to run Data Quality checks
using Soda Core.
"""
from typing import Dict, List, Optional, Union

from prefect import get_run_logger, task
from prefect_shell import shell_run_command

from prefect_soda_core.soda_configuration import SodaConfiguration
from prefect_soda_core.sodacl_check import SodaCLCheck


@task
async def soda_scan_execute(
    data_source_name: str,
    configuration: SodaConfiguration,
    checks: SodaCLCheck,
    variables: Optional[Dict[str, str]],
    verbose: bool = False,
) -> Union[List, str]:
    """
    Task that execute a Soda Scan.
    First, the scan is created and configured using the provided
    configuration, checks, and other options, and then
    it is executed against the provided data source.

    Args:
        data_source_name: The name of the data source against
            which the checks will be executed. The data source name
            must match one of the data sources provided in the
            `configuration` object.
        configuration: `SodaConfiguration` object that will be used
            to configure the scan before its execution.
        checks: `SodaCLCheck` object that will be used, together with
            `configuration`, to configure the scan before its execution.
        variables: A `Dict[str, str]` that contains all variables
            references within checks.
        verbose: Whether to run the checks with a verbose log or not.
            Default to `False`.

    Raises:
        `RuntimeError` in case `soda scan` encounters any error
            during execution.

    Returns:
        Logs produced by running `soda scan` CLI command.

    Example:
        ```python
        from prefect_soda_core.sodacl_check import SodaCLCheck
        from prefect_soda_core.soda_configuration import SodaConfiguration
        from prefect_soda_core.tasks import soda_scan_execute

        from prefect import flow

        sodacl_check_block = SodaCLCheck.load("SODACL_CHECK_BLOCK_NAME")
        soda_configuration_block = SodaConfiguration.load("SODA_CONF_BLOCK_NAME")

        @flow
        def run_soda_scan():
            return soda_scan_execute(
                data_source_name="datasource",
                configuration=soda_configuration_block,
                checks=sodacl_check_block,
                variables={"key": "value"},
                verbose=False
            )
        ```
    """
    # Persist the configuration on the file system, if necessary
    configuration.persist_configuration()

    # Perists checks on the file system, if necessary
    checks.persist_checks()

    # Soda command initial definition
    command = (
        f"soda scan -d {data_source_name} -c {configuration.configuration_yaml_path}"
    )

    # If variables are provided, add the to Soda command
    if variables:
        var_str = "".join(
            [
                f'-v "{var_name}={var_value}" '
                for var_name, var_value in variables.items()
            ]
        )

        command = f"{command} {var_str}"

    # If verbose logging is requested, add corresponding option to Soda command
    if verbose:
        command = f"{command} -V"

    # Build final Soda command
    command = f"{command} {checks.sodacl_yaml_path}"

    # Log Soda command for debuggin purpose
    get_run_logger().debug(f"Soda requested command is: {command}")

    # Execute Soda command
    soda_logs = await shell_run_command.fn(command=command, return_all=True)

    return soda_logs
