from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="airflow-provider-snowpark",
    version="0.0.1",
    description="A snowpark provider.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    entry_points={
        "apache_airflow_provider": [
            "provider_info=snowpark_provider.__init__:get_provider_info"
        ]
    },
    license="Apache License 2.0",
    packages=[
        "snowpark_provider",
        "snowpark_provider.hooks",
        "snowpark_provider.decorators",
        "snowpark_provider.utils",
    ],
    install_requires=[
        "apache-airflow>=2.0",
        "apache-airflow-providers-snowflake==4.0.2",
        "snowflake-snowpark-python[pandas]==1.0.0",
    ],
    setup_requires=["setuptools", "wheel"],
    author="Plinio Guzman",
    author_email="plinio@astronomer.io",
    url="http://astronomer.io/",
    classifiers=[
        "Framework :: Apache Airflow",
        "Framework :: Apache Airflow :: Provider",
    ],
    python_requires="==3.8.*",
)
