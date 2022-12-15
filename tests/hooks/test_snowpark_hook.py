"""
Unittest module to test Hooks.

Requires the unittest, pytest Python libraries.

Run test:

    python3 -m unittest tests.hooks.test_sample_hook.TestSnowparkHook

"""

import logging
import os
import unittest

import pytest

from snowpark_provider.hooks.snowpark_hook import SnowparkHook

log = logging.getLogger(__name__)


class TestSnowparkHook(unittest.TestCase):
    """
    Test Sample Hook.
    """

    def test_get_snowpark_session(self):

        # Instantiate hook
        hook = SnowparkHook(
            conn_id="snowflake_default",
        )

        # Create Snowpark session and test that a dataframe can be created
        snowpark_session = hook.get_snowpark_session()
        python_df = snowpark_session.create_dataframe(["a", "b", "c"])
        python_df.show()


if __name__ == "__main__":
    unittest.main()
