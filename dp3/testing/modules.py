"""Compatibility exports for DP3 secondary module testing helpers."""

from dp3.testing.case import DP3ModuleTestCase
from dp3.testing.config import CONFIG_DIR_ENV, resolve_config_dir
from dp3.testing.registrar import HookRegistration, TestCallbackRegistrar

__all__ = [
    "CONFIG_DIR_ENV",
    "DP3ModuleTestCase",
    "HookRegistration",
    "TestCallbackRegistrar",
    "resolve_config_dir",
]
