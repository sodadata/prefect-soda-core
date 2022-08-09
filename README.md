# prefect-soda-core-collection

## Welcome!

Prefect 2.0 collection for Soda Core

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-soda-core-collection` with `pip`:

```bash
pip install prefect-soda-core-collection
```

### Write and run a flow

```python
from prefect import flow
from prefect_soda_core_collection.tasks import (
    goodbye_prefect_soda_core_collection,
    hello_prefect_soda_core_collection,
)


@flow
def example_flow():
    hello_prefect_soda_core_collection
    goodbye_prefect_soda_core_collection

example_flow()
```

## Resources

If you encounter any bugs while using `prefect-soda-core-collection`, feel free to open an issue in the [prefect-soda-core-collection](https://github.com/sodadata/prefect-soda-core-collection) repository.

If you have any questions or issues while using `prefect-soda-core-collection`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

## Development

If you'd like to install a version of `prefect-soda-core-collection` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/sodadata/prefect-soda-core-collection.git

cd prefect-soda-core-collection/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
