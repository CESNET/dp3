import os
import sys

from pydantic import BaseModel, ValidationError, validator

from dp3.common.config import ModelSpec, read_config_dir
from dp3.database.database import EntityDatabase


class ConfigEnv(BaseModel):
    """Configuration environment variables container"""

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


# Validate environmental variables
try:
    conf_env = ConfigEnv.parse_obj(os.environ)

    # Load configuration, entity and attribute specification and connect to dp3 message broker.
    CONFIG = read_config_dir(conf_env.CONF_DIR, recursive=True)
    MODEL_SPEC = ModelSpec(CONFIG.get("db_entities"))
    DB = EntityDatabase(CONFIG.get("database"), MODEL_SPEC)
except ValidationError as e:
    print("Invalid or missing environmental variables:", file=sys.stderr)

    error_message_no_first_line = str(e).split("\n", 1)[-1]
    print(error_message_no_first_line, file=sys.stderr)

    sys.exit(1)
