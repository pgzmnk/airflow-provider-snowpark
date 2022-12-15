"""Setup.py for the Astronomer sample Airflow provider package. Built from datadog provider package for now."""

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

"""Perform the package airflow-provider-sample setup."""
setup(
    name="airflow-provider-snowpark",
    version="0.0.1",
    description="A snowpark provider.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=sample_provider.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=["sample_provider", "sample_provider.hooks", "sample_provider.decorators"],
    install_requires=["apache-airflow>=2.0"],
    setup_requires=["setuptools", "wheel"],
    author="Plinio Guzmon",
    author_email="plinio@astronomer.io",
    url="http://astronomer.io/",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires="~=3.7",
)
