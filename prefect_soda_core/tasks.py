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


def get_configured_scan(
    scan: Scan, configuration: SodaConfiguration, checks: SodaCLCheck
) -> Scan:
    """
    TODO
    """

    # Add scan configuration
    if configuration.configuration_yaml_file:
        scan.add_configuration_yaml_file(
            file_path=configuration.configuration_yaml_file
        )
    elif configuration.configuration_yaml_str:
        scan.add_configuration_yaml_str(
            environment_yaml_str=configuration.configuration_yaml_str
        )

    # Add checks
    if checks.sodacl_yaml_files:
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
    TODO
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
    scan = get_configured_scan(scan=scan, configuration=configuration, checks=checks)

    ret = scan.execute()

    # In case of errors, raise an exception with Soda errors
    if ret == 3:
        errors = scan.get_error_logs_text()
        msg = f"Soda scan encountered an error: {errors}"
        raise SodaScanRunException(msg)

    return scan
