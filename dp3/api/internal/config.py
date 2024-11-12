import os
import sys
from enum import Enum

from pydantic import (
    BaseModel,
    ValidationError,
    field_validator,
)

from dp3.api.internal.dp_logger import DPLogger
from dp3.common.config import ModelSpec, read_config_dir
from dp3.database.database import EntityDatabase
from dp3.history_management.telemetry import TelemetryReader
from dp3.task_processing.task_queue import TaskQueueWriter

DATAPOINTS_INGESTION_URL_PATH = "/datapoints"


class ConfigEnv(BaseModel):
    """Configuration environment variables container"""

    APP_NAME: str
    CONF_DIR: str
    ROOT_PATH: str = ""

    @field_validator("CONF_DIR")
    @classmethod
    def validate(cls, v):
        # Try to open config
        try:
            config = read_config_dir(v, recursive=True)
        except OSError as e:
            raise ValueError(f"Cannot open: {v}") from e

        assert "db_entities" in config, "Config for 'db_entities' is missing"
        assert "database" in config, "Config for 'database' is missing"

        # This may raise ValueError too
        ModelSpec(config.get("db_entities"))

        return v


try:
    # Validate and parse environmental variables
    conf_env = ConfigEnv.parse_obj(os.environ)
except ValidationError as e:
    config_error = any("CONF_DIR" in x["loc"] and len(x["loc"]) > 1 for x in e.errors())
    env_error = any(len(x["loc"]) == 1 for x in e.errors())
    print(
        ("Invalid or missing environmental variables" if env_error else "")
        + (" && " if env_error and config_error else "")
        + ("Invalid model specification (check entity config)" if config_error else "")
        + ":",
        file=sys.stderr,
    )

    error_message_no_first_line = str(e).split("\n", 1)[-1]
    print(error_message_no_first_line, file=sys.stderr)

    sys.exit(1)

# Load configuration, entity and attribute specification and connect to DP3 message broker.
CONFIG = read_config_dir(conf_env.CONF_DIR, recursive=True)
EnabledModules = Enum(
    "EnabledModules", {module: module for module in CONFIG.get("processing_core.enabled_modules")}
)
MODEL_SPEC = ModelSpec(CONFIG.get("db_entities"))
DB = EntityDatabase(CONFIG, MODEL_SPEC, CONFIG.get("processing_core.worker_processes"))
TASK_WRITER = TaskQueueWriter(
    conf_env.APP_NAME,
    CONFIG.get("processing_core.worker_processes"),
    CONFIG.get("processing_core.msg_broker"),
)
CONTROL_WRITER = TaskQueueWriter(
    conf_env.APP_NAME,
    CONFIG.get("processing_core.worker_processes"),
    CONFIG.get("processing_core.msg_broker"),
    exchange=f"{conf_env.APP_NAME}-control-exchange",
)
DP_LOGGER = DPLogger(CONFIG.get("api.datapoint_logger"))
ROOT_PATH = conf_env.ROOT_PATH
TELEMETRY_READER = TelemetryReader(DB)
