<p align="center">
  <a href="https://www.airflow.apache.org">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
  </a>
</p>
<h1 align="center">
  Airflow Snowpark Provider
</h1>
  <h3 align="center">
    Interact with Snowpark dataframes with the Airflow snowpark decorator.
</h3>

<br/>

## Description

**Snowflake Decorator** is a Python SDK to develop ETL and ML workflows in [Apache Airflow](https://airflow.apache.org/) with Snowpark. It resolves XCom dependencies between tasks as you write workflows in a simplified syntax. 

## Roadmap

The SDK currently exposes the Snowpark `@dataframe_decorator` to share Snowpark Dataframes between tasks.

There are plans to create the `@declare_udf` decorator to declare user defined functions as tasks, and the `@stored_procedure` decorator to execute code as stored procedures.

### Repository structure


```bash
├── LICENSE 
├── README.md
├── snowpark_provider
│   ├── __init__.py
│   ├── example_dags
│   │   ├── __init__.py
│   │   └── dataframe_poc_dag.py
│   ├── hooks
│   │   ├── __init__.py
│   │   └── snowpark_hook.py
│   ├── decorators
│   │   ├── __init__.py
│   │   ├── dataframe_decorator.py
│   │   └── base_decorator.py
│   └── utils
│       ├── __init__.py
│       └── table.py
├── setup.py
└── tests # To-do
    ├── __init__.py
    ├── test_utils.py
    ├── hooks
    │   ├── __init__.py
    │   └── test_snowpark_hook.py
    └── decorators
        ├── __init__.py
        └── test_snowpark_decorator.py
```

