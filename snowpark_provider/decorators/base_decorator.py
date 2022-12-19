from __future__ import annotations

import os
import inspect
import logging
from typing import Any, Callable, Sequence

from airflow.decorators.base import DecoratedOperator, task_decorator_factory
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator

from snowpark_provider.hooks.snowpark_hook import SnowparkHook
from snowpark_provider.utils.table import find_first_table, Table

try:
    from airflow.decorators.base import TaskDecorator, task_decorator_factory
except ImportError:
    from airflow.decorators import _TaskDecorator as TaskDecorator
    from airflow.decorators.base import task_decorator_factory

try:
    import snowflake.snowpark  # noqa
except ImportError:
    raise AirflowException(
        "The snowflake-snowpark-python package is not installed. Make sure you are using Python 3.8."
    )

try:
    from airflow.utils.context import Context
except ModuleNotFoundError:

    class Context(MutableMapping[str, Any]):
        """Placeholder typing class for ``airflow.utils.context.Context``."""


from snowpark_provider.utils.table import Table

os.environ[
    "AIRFLOW__CORE__ALLOWED_DESERIALIZATION_CLASSES"
] = "snowpark_provider.utils.table.Table"


class BaseSnowparkOperator(DecoratedOperator, PythonOperator):
    """
    Wraps a Python callable and captures args/kwargs when called for execution.

    :param conn_id: Reference to
        :ref:`Snowflake connection id<howto/connection:snowflake>`
    :param parameters: (optional) the parameters to render the SQL query with.
    :param warehouse: name of warehouse (will overwrite any warehouse defined in the connection's extra JSON)
    :param database: name of database (will overwrite database defined in connection)
    :param schema: name of schema (will overwrite schema defined in connection)
    :param role: name of role (will overwrite any role defined in connection's extra JSON)
    :param authenticator: authenticator for Snowflake.
        'snowflake' (default) to use the internal Snowflake authenticator
        'externalbrowser' to authenticate using your web browser and
        Okta, ADFS or any other SAML 2.0-compliant identify provider
        (IdP) that has been defined for your account
        'https://<your_okta_account_name>.okta.com' to authenticate
        through native Okta.
    :param session_parameters: You can set session-level parameters at the time you connect to Snowflake
    :param python_callable: A reference to an object that is callable
    :param op_kwargs: a dictionary of keyword arguments that will get unpacked in your function (templated)
    :param op_args: a list of positional arguments that will get unpacked when
        calling your callable (templated)
    :return If the return value of the handler function is a Snowpark dataframe,
      it's converted to temporary Snowflake table.
    """

    TMP_SCHEMA = "tmp_astro"

    custom_operator_name = "snowpark_task"

    template_fields: Sequence[str] = ("op_args", "op_kwargs")

    # since we won't mutate the arguments, we should just do the shallow copy
    # there are some cases we can't deepcopy the objects (e.g protobuf).
    shallow_copy_attrs: Sequence[str] = ("python_callable",)

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
        self.conn_id = conn_id
        self.parameters = parameters
        self.warehouse = warehouse
        self.database = database
        self.role = role
        self.schema = schema
        self.authenticator = authenticator
        self.session_parameters = session_parameters
        self.columns_names_capitalization = "original"

        self.op_args: dict[str, Table] = {}  # To-do: intreduce pd.DataFrame

        kwargs_to_upstream = {
            "python_callable": python_callable,
            "op_args": op_args,
            "op_kwargs": op_kwargs,
        }
        super().__init__(
            kwargs_to_upstream=kwargs_to_upstream,
            python_callable=python_callable,
            op_args=op_args,
            # airflow.decorators.base.DecoratedOperator checks if the functions are bindable, so we have to
            # add an artificial value to pass the validation. The real value is determined at runtime.
            op_kwargs={**op_kwargs, "snowpark_session": None},
            **kwargs,
        )

    def execute(self, context: Context) -> Table | list:
        first_table = find_first_table(
            op_args=self.op_args,  # type: ignore
            op_kwargs=self.op_kwargs,
            python_callable=self.python_callable,
            parameters=self.parameters or {},  # type: ignore
            context=context,
        )
        if first_table:
            self.conn_id = self.conn_id or first_table.conn_id  # type: ignore
            self.database = self.database or first_table.metadata.get("database") # first_table.metadata.database # to-do: or first_table.metadata.get("database")  # type: ignore
            self.schema = self.schema or first_table.metadata.get("schema") # first_table.metadata.schema # to-do: or first_table.metadata.get("schema")  # type: ignore
        else:
            if not self.conn_id:
                raise ValueError("You need to provide a table or a connection id")

        # Set real sessions as an argument to the function.
        hook = SnowparkHook(snowflake_conn_id=self.conn_id)
        conn = hook.get_conn()
        snowpark_session = hook.get_snowpark_session()
        op_kwargs = dict(self.op_kwargs)

        # Load dataframes from arguments tagged with `Table`.
        self.op_args = load_op_arg_table_into_dataframe(
            self.op_args,
            self.python_callable,
            self.columns_names_capitalization,
            self.log,
        )
        self.op_kwargs = load_op_kwarg_table_into_dataframe(
            self.op_kwargs,
            self.python_callable,
            self.columns_names_capitalization,
            self.log,
        )
        self.op_kwargs["snowpark_session"] = snowpark_session

        function_output = self.python_callable(
         *self.op_args, **self.op_kwargs
        )

        if function_output:
            if isinstance(
                function_output, snowflake.snowpark.dataframe.DataFrame
            ) or isinstance(function_output, snowflake.snowpark.table.Table):
                if isinstance(function_output, snowflake.snowpark.dataframe.DataFrame):
                    # Snowpark DataFrames stored in temp tables, in temp schema
                    _table_name = "__".join(["TMP", self.dag_id, self.task_id])
                    table_name = ".".join([conn.database, self.TMP_SCHEMA, _table_name])
                elif isinstance(function_output, snowflake.snowpark.table.Table):
                    table_name = function_output.table_name
                function_output.write.mode("overwrite").save_as_table(table_name)
                output = Table(name=table_name, conn_id=self.conn_id)
            else:
                output = function_output

            try:
                return output
            finally:
                snowpark_session.close()


def load_op_arg_table_into_dataframe(
    op_args: tuple,
    python_callable: Callable,
    columns_names_capitalization: ColumnCapitalization,
    log: logging.Logger,
) -> tuple:
    """For dataframe based functions, takes any Table objects from the op_args
    and converts them into local dataframes that can be handled in the python context"""
    full_spec = inspect.getfullargspec(python_callable)
    op_args_list = list(op_args)
    ret_args = []
    # We check if the type annotation is of type dataframe to determine that the user actually WANTS
    # this table to be converted into a dataframe, rather that passed in as a table
    log.debug("retrieving op_args")

    for arg in op_args_list:
        current_arg = full_spec.args.pop(0)
        if full_spec.annotations.get(current_arg) == Table:
            log.debug("Found Snowpark Table, retrieving dataframe from table %s", arg)
            snowpark_session = SnowparkHook(snowflake_conn_id=arg.conn_id).get_snowpark_session()
            ret_args.append(snowpark_session.table(arg.name))
        else:
            print("Did not find Snowpark Table, passing raw value: %s", arg)
            ret_args.append(arg)
    return tuple(ret_args)


def load_op_kwarg_table_into_dataframe(
    op_kwargs: dict,
    python_callable: Callable,
    columns_names_capitalization: ColumnCapitalization,
    log: logging.Logger,
) -> dict:
    """For dataframe based functions, takes any Table objects from the op_kwargs
    and converts them into local dataframes that can be handled in the python context"""
    param_types = inspect.signature(python_callable).parameters
    # We check if the type annotation is of type dataframe to determine that the user actually WANTS
    # this table to be converted into a dataframe, rather that passed in as a table
    out_dict = {}
    log.debug("retrieving op_kwargs")
    for (
        k,
        v,
    ) in op_kwargs.items():  # e.g. def task_func(df1: Table = df1, df2: Table = df2):
        # If the typed table is a Snowpark Table, retrieve the dataframe
        if param_types.get(k).annotation == Table:
            log.debug(
                "Found Snowpark Table, retrieving dataframe from table %s", v.name
            )
            print("Found Snowpark Table, retrieving dataframe from table %s", v.name)
            snowpark_session = SnowparkHook(snowflake_conn_id=v.conn_id).get_snowpark_session()
            out_dict[k] = snowpark_session.table(v.name)
        # To-do: Handle pandas dataframe arguments
        else:
            print("Did not find Snowpark Table, passing raw value: %s", v.name)
            out_dict[k] = v
    return out_dict
