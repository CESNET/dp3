import os
import sys

from pydantic import BaseModel, ValidationError, validator

from dp3.common.config import ModelSpec, read_config_dir
from dp3.database.database import EntityDatabase
from dp3.task_processing.task_queue import TaskQueueWriter


class ConfigEnv(BaseModel):
    """Configuration environment variables container"""

    APP_NAME: str
    CONF_DIR: str

    @validator("CONF_DIR")
    def validate(cls, v, values):
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
    print("Invalid or missing environmental variables:", file=sys.stderr)

    error_message_no_first_line = str(e).split("\n", 1)[-1]
    print(error_message_no_first_line, file=sys.stderr)

    sys.exit(1)

# Load configuration, entity and attribute specification and connect to DP3 message broker.
CONFIG = read_config_dir(conf_env.CONF_DIR, recursive=True)
MODEL_SPEC = ModelSpec(CONFIG.get("db_entities"))
DB = EntityDatabase(CONFIG.get("database"), MODEL_SPEC)
TASK_WRITER = TaskQueueWriter(
    conf_env.APP_NAME,
    CONFIG.get("processing_core.worker_processes"),
    CONFIG.get("processing_core.msg_broker"),
)
