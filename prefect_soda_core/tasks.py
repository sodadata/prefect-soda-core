"""
Collection of tasks that can be used to run Data Quality checks
using Soda Core.
"""
from typing import Dict, Optional

from prefect import task
from soda.scan import Scan

from prefect_soda_core.exceptions import SodaScanRunException
from prefect_soda_core.soda_configuration import SodaConfiguration
from prefect_soda_core.sodacl_check import SodaCLCheck


def __get_configured_scan(
    scan: Scan, configuration: SodaConfiguration, checks: SodaCLCheck
) -> Scan:
    """
    Configure a Soda scan using the provided configuration and checks.

    Args:
        scan: `Scan` object to configure.
        configuration: `SodaConfiguration` object that contains configuration
            details to be used to configure `scan`.
        checks: `SodaCLCheck` object that contains checks details to be used
            to configure `scan`.

    Returns:
        A properly configured `Scan` object.
    """

    # Add scan configuration
    if configuration.configuration_yaml_file:
        scan.add_configuration_yaml_file(
            file_path=configuration.configuration_yaml_file
        )
    elif configuration.configuration_yaml_files:
        scan.add_configuration_yaml_files(path=configuration.configuration_yaml_files)
    elif configuration.configuration_yaml_str:
        scan.add_configuration_yaml_str(
            environment_yaml_str=configuration.configuration_yaml_str
        )

    # Add checks
    if checks.sodacl_yaml_file:
        scan.add_sodacl_yaml_file(file_path=checks.sodacl_yaml_file)
    elif checks.sodacl_yaml_files:
        scan.add_sodacl_yaml_files(path=checks.sodacl_yaml_files)
    elif checks.sodacl_yaml_str:
        scan.add_sodacl_yaml_str(sodacl_yaml_str=checks.sodacl_yaml_str)

    return scan


@task
def soda_scan_execute(
    data_source_name: str,
    configuration: SodaConfiguration,
    checks: SodaCLCheck,
    variables: Optional[Dict[str, str]],
    verbose: bool = False,
    disable_telemetry: bool = False,
) -> Scan:
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
        disable_telemetry: Whether to disable telemetry or not.
            Default to `False`. For more information about
            Soda telemetry, refer to the
            [official docs](https://docs.soda.io/soda-core/usage-stats.html)

    Returns:
        `Scan` object containing the result collected after its execution.

    Raises:
        `SodaScanRunException` in case of Soda execution failure.
    """

    # Init Soda scan object
    scan = Scan()

    # Set the main data source where checks will be performed
    scan.set_data_source_name(data_source_name=data_source_name)

    # Set log verbosity
    scan.set_verbose(verbose_var=verbose)

    # Add scan variables, if any
    if variables:
        scan.add_variables(variables=variables)

    # Disable telemetry if needed
    if disable_telemetry:
        scan.disable_telemetry()

    # Configure the scan based on configuration and checks
    scan = __get_configured_scan(scan=scan, configuration=configuration, checks=checks)

    ret = scan.execute()

    # In case of errors, raise an exception with Soda errors
    if ret == 3:
        errors = scan.get_error_logs_text()
        msg = f"Soda scan encountered an error: {errors}"
        raise SodaScanRunException(msg)

    return scan
