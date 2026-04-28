"""Configuration helpers for DP3 module tests."""

import os
from contextlib import suppress
from typing import Optional

from dp3.common.config import HierarchicalDict, ModelSpec, PlatformConfig, read_config_dir

CONFIG_DIR_ENV = "DP3_CONFIG_DIR"


def resolve_config_dir(config_dir: Optional[str] = None, env_var: str = CONFIG_DIR_ENV) -> str:
    """Return an absolute DP3 config directory path.

    Explicit ``config_dir`` values take precedence. If no explicit path is supplied, the path is
    read from ``env_var``.
    """
    resolved = config_dir or os.environ.get(env_var)
    if not resolved:
        raise ValueError(
            f"DP3 module tests require a config directory. Set {env_var} or pass "
            "config_dir explicitly."
        )
    return os.path.abspath(resolved)


def load_config(
    config_dir: Optional[str] = None, env_var: str = CONFIG_DIR_ENV
) -> HierarchicalDict:
    """Load a DP3 config directory for module tests."""
    return read_config_dir(resolve_config_dir(config_dir, env_var), recursive=True)


def build_model_spec(config: HierarchicalDict) -> ModelSpec:
    """Build a model specification from loaded DP3 configuration."""
    return ModelSpec(config.get("db_entities"))


def build_platform_config(
    config: HierarchicalDict,
    model_spec: ModelSpec,
    config_dir: Optional[str] = None,
    *,
    app_name: str = "test",
    process_index: int = 0,
    num_processes: int = 1,
    env_var: str = CONFIG_DIR_ENV,
) -> PlatformConfig:
    """Build the minimal platform config needed by secondary module unit tests."""
    with suppress(Exception):
        num_processes = config.get("processing_core.worker_processes", num_processes)
    return PlatformConfig(
        app_name=app_name,
        config_base_path=resolve_config_dir(config_dir, env_var),
        config=config,
        model_spec=model_spec,
        process_index=process_index,
        num_processes=num_processes,
    )


def get_module_config(config: HierarchicalDict, module_name: Optional[str]) -> dict:
    """Return module-specific config from loaded app config."""
    if module_name is None:
        return {}
    return config.get(f"modules.{module_name}", {})
