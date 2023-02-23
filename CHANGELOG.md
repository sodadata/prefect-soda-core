# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## Unreleased

### Added

### Changed

### Deprecated

### Removed

### Fixed

### Security

## 0.1.0

Released on ????? ?th, 20??.

### Added

- `task_name` task - [#1](https://github.com/sodadata/prefect-soda-core/pull/1)


## 0.1.1

Released on ????? ?th, 20??.

### Changed

The following changes have been made to `soda_scan_execute`:
- Introducing the optional `return_scan_result_file_content` flag which if it's set to `True` will return the scan result file content as a dictionary. This is useful when you want to use the scan result file content in a downstream task, to send alerts independednt of soda-cloud for example.
- Introducing the optional `scan_results_file` parameter which if it's set to a path, will save the scan result file to the specified path. This is useful when you want to save the scan result file to a specific location on the machine running the task. If `return_scan_result_file_content` is set to `True` but no `scan_results_file` is specified, the scan result file will be saved to the current working directory using the task run name and timestamp as the file name.
- Introducing the optional `env_vars` parameter which if it's set to a dictionary, will pass the dictionary as environment variables to the shell task running the soda scan command. This is useful when you want to pass database credentials to the soda scan task. For example, if you want to scan a snowflake database, [you can pass the snowflake credentials such as account and password as environment variables to the soda scan task](https://docs.soda.io/soda/connect-snowflake.html#configuration). 

### Fixed
- In the `soda_scan_execute` task, handling the `RuntimeError` exception that is raised when the check runs successfully but does not pass, causing the flow running it to fail. This is the only way to handle this exception when the soda_scan_execute in a [mapped](https://docs.prefect.io/api-ref/prefect/tasks/#prefect.tasks.Task.map) mode.


