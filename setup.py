from setuptools import find_packages, setup

import versioneer

with open("requirements.txt") as install_requires_file:
    install_requires = install_requires_file.read().strip().split("\n")

with open("requirements-dev.txt") as dev_requires_file:
    dev_requires = dev_requires_file.read().strip().split("\n")

with open("README.md") as readme_file:
    readme = readme_file.read()


def get_extra_requires():
    # DB engines currently supported by Soda Core
    # https://docs.soda.io/soda-core/installation.html#install
    db_engines = {
        "athena",
        "redshift",
        "spark-df",
        "bigquery",
        "db2",
        "sqlserver",
        "mysql",
        "postgres",
        "snowflake",
        "trino",
    }

    # Generate extra requires for each db engine
    extra_requires = {db_engine: f"soda-core-{db_engine}" for db_engine in db_engines}

    # Does not work with the current cli-based integration
    # extra_requires["spark-hive"] = "soda-core-spark[hive]"
    # extra_requires["spark-odbc"] = "soda-core-spark[odbc]"

    # Add dev deps
    extra_requires["dev"] = dev_requires

    return extra_requires


setup(
    name="prefect-soda-core",
    description="Prefect 2.0 collection for Soda Core",
    license="Apache License 2.0",
    author="Soda Data NV.",
    author_email="vijay@soda.io",
    keywords="prefect",
    url="https://github.com/sodadata/prefect-soda-core",
    long_description=readme,
    long_description_content_type="text/markdown",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    packages=find_packages(exclude=("tests", "docs")),
    python_requires=">=3.7",
    install_requires=install_requires,
    extras_require=get_extra_requires(),
    classifiers=[
        "Natural Language :: English",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: Apache Software License",
        "Programming Language :: Python :: 3 :: Only",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Libraries",
    ],
)
