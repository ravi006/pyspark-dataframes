"""Sonobi common exception definitions."""

from __future__ import absolute_import


class TestConfigError(Exception):
    """When Failed to get correct config parameters."""

    pass


class TestConnectionError(Exception):
    """Use when unable to establish connection."""

    pass


class TestParameterError(Exception):
    """Use when mandatory parameter is missing or value is incorrect."""

    pass


class TestFileError(Exception):
    """Use when File Reading/ Writing fail."""

    pass
