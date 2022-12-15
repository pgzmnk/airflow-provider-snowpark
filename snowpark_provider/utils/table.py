from __future__ import annotations

import inspect
import random
import string
from typing import Any, Callable

from airflow.models.xcom_arg import XComArg
from attr import define, field, fields_dict
from sqlalchemy import Column, MetaData
import os


@define
class Metadata:
    """
    Contains additional information to access a SQL Table, which is very likely optional and, in some cases, may
    be database-specific.
    :param schema: A schema name
    :param database: A database name
    """

    # This property is used by several databases, including: Postgres, Snowflake and BigQuery ("namespace")
    _schema: str | None = None
    database: str | None = None
    region: str | None = None

    def is_empty(self) -> bool:
        """Check if all the fields are None."""
        return all(
            getattr(self, field_name) is None
            for field_name in fields_dict(self.__class__)
        )

    @property
    def schema(self):
        if self.region:
            # We are replacing the `-` with `_` because for bigquery doesn't allow `-` in schema name
            return f"{self._schema}__{self.region.replace('-', '_')}"
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value


@define(slots=False)
class BaseTable:
    """
    Base class that has information necessary to access a SQL Table. It is agnostic to the database type.
    If no name is given, it auto-generates a name for the Table and considers it temporary.
    Temporary tables are prefixed with the prefix TEMP_PREFIX.
    :param name: The name of the database table. If name not provided then it would create a temporary name
    :param conn_id: The Airflow connection id. This will be used to identify the right database type at the runtime
    :param metadata: A metadata object which will have database or schema name
    :param columns: columns which define the database table schema.
    :sphinx-autoapi-skip:
    """

    MAX_TABLE_NAME_LENGTH = 62

    template_fields = ("name",)

    name: str = field(default="")
    conn_id: str = field(default="")
    # Setting converter allows passing a dictionary to metadata arg
    metadata: Metadata = field(
        factory=Metadata,
        # converter=metadata_field_converter,
    )
    columns: list[Column] = field(factory=list)
    temp: bool = field(default=False)

    def __attrs_post_init__(self) -> None:
        TEMP_PREFIX = "_tmp"
        if not self.name:
            self.name = self._create_unique_table_name(TEMP_PREFIX + "_")
            self.temp = True
        if self.name.startswith(TEMP_PREFIX):
            self.temp = True

    # We need this method to pickle Table object, without this we cannot push/pull this object from xcom.
    def __getstate__(self):
        return self.__dict__

    def _create_unique_table_name(self, prefix: str = "") -> str:
        """
        If a table is instantiated without a name, create a unique table for it.
        This new name should be compatible with all supported databases.
        """
        MAX_TABLE_NAME_LENGTH = 62

        schema_length = len((self.metadata and self.metadata.schema) or "") + 1
        prefix_length = len(prefix)

        unique_id = random.choice(string.ascii_lowercase) + "".join(
            random.choice(string.ascii_lowercase + string.digits)
            for _ in range(MAX_TABLE_NAME_LENGTH - schema_length - prefix_length)
        )
        if prefix:
            unique_id = f"{prefix}{unique_id}"

        return unique_id

    def to_json(self):
        return {
            "class": "Table",
            "name": self.name,
            "metadata": {
                "schema": self.metadata.schema,
                "database": self.metadata.database,
            },
            "temp": self.temp,
            "conn_id": self.conn_id,
        }

    @classmethod
    def from_json(cls, obj: dict):
        return Table(
            name=obj["name"],
            metadata=obj["metadata"],
            temp=obj["temp"],
            conn_id=obj["conn_id"],
        )


@define(slots=False)
class TempTable(BaseTable):
    """
    Internal class to represent a Temporary table
    """

    temp: bool = field(default=True)


@define(slots=False)
class Table(BaseTable):
    """
    User-facing class that has information necessary to access a SQL Table. It is agnostic to the database type.
    If no name is given, it auto-generates a name for the Table and considers it temporary.
    Temporary tables are prefixed with the prefix TEMP_PREFIX.
    :param name: The name of the database table. If name not provided then it would create a temporary name
    :param conn_id: The Airflow connection id. This will be used to identify the right database type at the runtime
    :param metadata: A metadata object which will have database or schema name
    :param columns: columns which define the database table schema.
    """

    # uri: str = field(init=False)
    extra: dict | None = field(init=False, factory=dict)

    def __new__(cls, *args, **kwargs):
        name = kwargs.get("name") or args and args[0] or ""
        temp = kwargs.get("temp", False)
        if temp or (not name or name.startswith("_tmp")):
            return TempTable(*args, **kwargs)
        return super().__new__(cls)


def _have_same_conn_id(tables: list[BaseTable]) -> bool:
    """
    Check to see if all tables belong to same conn_id. Otherwise, this can go wrong for cases
    1. When we have tables from different DBs.
    2. When we have tables from different conn_id, since they can be configured with different database/schema etc.
    :param tables: tables to check for conn_id field
    :return: True if all tables have the same conn id, and False if not.
    """
    return len(tables) == 1 or len({table.conn_id for table in tables}) == 1


def _find_first_table_from_op_args(
    op_args: tuple, context: Context
) -> BaseTable | None:
    """
    Read op_args and extract the tables.
    :param op_args: user-defined operator's args
    :param context: the context to use for resolving XComArgs
    :return: first valid table found in op_args.
    """
    args = [
        arg.resolve(context) if isinstance(arg, XComArg) else arg for arg in op_args
    ]
    tables = [arg for arg in args if isinstance(arg, BaseTable)]

    if _have_same_conn_id(tables):
        return tables[0]
    return None


def _find_first_table_from_op_kwargs(
    op_kwargs: dict, python_callable: Callable, context: Context
) -> BaseTable | None:
    """
    Read op_kwargs and extract the tables.
    :param op_kwargs: user-defined operator's kwargs
    :param context: the context to use for resolving XComArgs
    :return: first valid table found in op_kwargs.
    """
    kwargs = [
        op_kwargs[kwarg.name].resolve(context)
        if isinstance(op_kwargs[kwarg.name], XComArg)
        else op_kwargs[kwarg.name]
        for kwarg in inspect.signature(python_callable).parameters.values()
        if kwarg.name != "snowpark_session"
    ]
    tables = [kwarg for kwarg in kwargs if isinstance(kwarg, BaseTable)]

    if _have_same_conn_id(tables):
        return tables[0]
    return None


def _find_first_table_from_parameters(
    parameters: dict, context: Context
) -> BaseTable | None:
    """
    Read parameters and extract the tables.
    :param parameters: a user-defined dictionary of parameters
    :param context: the context to use for resolving XComArgs
    :return: first valid table found in parameters.
    """
    params = [
        param.resolve(context) if isinstance(param, XComArg) else param
        for param in parameters.values()
    ]
    tables = [param for param in params if isinstance(param, BaseTable)]

    if _have_same_conn_id(tables):
        return tables[0]
    return None


def find_first_table(
    op_args: tuple,
    op_kwargs: dict,
    python_callable: Callable,
    parameters: dict,
    context: Context,
) -> BaseTable | None:
    """
    When we create our SQL operation, we run with the assumption that the first table given is the "main table".
    This means that a user doesn't need to define default conn_id, database, etc. in the function unless they want
    to create default values.
    :param op_args: user-defined operator's args
    :param op_kwargs: user-defined operator's kwargs
    :param python_callable: user-defined operator's callable
    :param parameters: user-defined parameters to be injected into SQL statement
    :param context: the context to use for resolving XComArgs
    :return: the first table declared as decorator arg or kwarg
    """
    first_table: BaseTable | None = None

    if op_args:
        first_table = _find_first_table_from_op_args(op_args=op_args, context=context)
    if not first_table and op_kwargs and python_callable is not None:
        first_table = _find_first_table_from_op_kwargs(
            op_kwargs=op_kwargs,
            python_callable=python_callable,
            context=context,
        )
    if not first_table and parameters:
        first_table = _find_first_table_from_parameters(
            parameters=parameters,
            context=context,
        )

    return first_table
