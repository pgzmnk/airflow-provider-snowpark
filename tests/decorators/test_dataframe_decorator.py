"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_sample_operator.TestSampleOperator

"""

import json
import logging
import os
import unittest
from unittest import mock

import pytest
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.utils import timezone

from snowpark_provider.decorators.dataframe_decorator import dataframe_decorator
from snowpark_provider.example_dags.dataframe_poc_dag import dag_obj
from tests import test_utils

log = logging.getLogger(__name__)

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestSnowparkDecorator(unittest.TestCase):
    """
    Test Snowpark Decorator.
    """

    def test_transform(self):

        test_utils.run_dag(dag_obj)


def sample_dag():
    dag_id = "test_dag"
    yield DAG(dag_id, start_date=DEFAULT_DATE)
    with create_session() as session_:
        session_.query(DagRun).delete()
        session_.query(TI).delete()
