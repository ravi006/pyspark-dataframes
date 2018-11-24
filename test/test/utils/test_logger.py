"""Tests for Log Util."""
from __future__ import absolute_import

from unittest import TestCase

import mock

# from doubles import expect

from test.utils.logger import TestLogger


class TestLogUtil(TestCase):
    """Test Log Util Class."""

    def setUp(self):
        """Init."""
        self.logger = TestLogger(__name__)

        self.values = {
            'test_kwarg1': 'kwarg1_value',
        }

    def test_info(self):
        self.logger.info(arg=["a"], kwargs=self.values)

    def test_error(self):
        self.logger.error(arg=["a"], kwargs=self.values)

    def test_warn(self):
        self.logger.warn(arg=["a"], kwargs=self.values)

    def test_construct_log(self):
        log_entry = self.logger.construct_log(["a"], kwargs=self.values)
        assert log_entry.get("test_kwarg1") == "kwarg1_value"
        assert isinstance(log_entry, dict)
