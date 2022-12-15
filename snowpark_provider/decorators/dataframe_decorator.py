from __future__ import annotations

try:
    from airflow.decorators.base import TaskDecorator, task_decorator_factory
except ImportError:
    from airflow.decorators.base import task_decorator_factory
    from airflow.decorators import _TaskDecorator as TaskDecorator

from snowpark_provider.decorators.base_decorator import BaseSnowparkOperator


class SnowparkDataframeOperator(BaseSnowparkOperator):
    """
    Run a task with a `snowpark_session` and return a Snowpark dataframe.
    """

    def __init__(
        self,
        *,
        conn_id: str = "snowflake_default",
        parameters: dict | None = None,
        warehouse: str | None = None,
        database: str | None = None,
        role: str | None = None,
        schema: str | None = None,
        authenticator: str | None = None,
        session_parameters: dict | None = None,
        python_callable,
        op_args,
        op_kwargs: dict,
        **kwargs,
    ) -> None:
        self.task_id = kwargs.get("task_id")
        self.conn_id = conn_id
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters

        super().__init__(
            task_id=self.task_id,
            conn_id=self.conn_id,
            parameters=self.parameters,
            warehouse=self.warehouse,
            database=self.database,
            role=self.role,
            schema=self.schema,
            authenticator=self.authenticator,
            session_parameters=self.session_parameters,
            python_callable=python_callable,
            op_args=op_args,
            op_kwargs=op_kwargs,
        )

    # To-do: custom execute for each decorator
    # def execute(self, context: Context) -> Table | list:


def dataframe_decorator(
    python_callable: Callable | None = None,
    multiple_outputs: bool | None = None,
    conn_id: str = "",
    database: str | None = None,
    schema: str | None = None,
    **kwargs,
) -> TaskDecorator:
    """Enables Snowspark tasks.

    Returned Snowspark dataframes are stored either as Snowflake tables or
    serialized Pandas dataframes - depending on the need of downstream tasks.

    By identifying the parameters as `Table` objects, astro knows to automatically convert those
    objects into tables (if they are, for example, a Snowpark dataframe). Any type besides
    table will lead to assume you do not want the parameter converted.

    :param python_callable: Function to decorate
    :param multiple_outputs: If set to True, the decorated function's return value will be unrolled to
        multiple XCom values. Dict will unroll to XCom values with its keys as XCom keys. Defaults to False.
    """
    kwargs.update(
        {
            "conn_id": conn_id,
            "database": database,
            "schema": schema,
        }
    )
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=multiple_outputs,
        decorated_operator_class=SnowparkDataframeOperator,
        **kwargs,
    )
