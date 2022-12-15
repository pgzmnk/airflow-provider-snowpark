import snowflake.snowpark
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


class SnowparkHook(SnowflakeHook):
    """
    Snowpark Hook that can return a Snowpark session.

    :param conn_id: Snowpark connection id
    :type conn_id: str
    """

    conn_name_attr = "conn_id"
    default_conn_name = "snowflake_default"
    conn_type = "snowflake"
    hook_name = "Snowpark"

    def __init__(self, *args, **kwargs) -> None:
        self.snowflake_conn_id = kwargs.get("snowflake_conn_id")
        self.conn_id = kwargs.get("conn_id")

        super().__init__(*args, **kwargs)

    def get_snowpark_session(self) -> snowflake.snowpark.Session:
        try:
            from snowflake.snowpark import Session as SnowparkSession
        except ImportError:
            SnowparkSession = None
        return SnowparkSession.builder.configs(self._get_conn_params()).create()
