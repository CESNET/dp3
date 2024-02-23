"""Core module that executes periodic update callbacks."""

import logging
from datetime import timedelta
from typing import Callable, Union

from pydantic import BaseModel, validate_call

from dp3.common.config import CronExpression, PlatformConfig
from dp3.common.scheduler import Scheduler
from dp3.common.task import DataPointTask
from dp3.common.types import ParsedTimedelta
from dp3.database.database import EntityDatabase


class UpdaterConfig(BaseModel):
    """The configuration of the Updater module.

    The periodic update is executed in smaller batches for better robustness.
    The batch size is dynamically adjusted based on the current number of
    processed entities, the total number of entities and the estimated growth rate.
    The minimum batch size can be also specified to avoid excessive overhead.

    Attributes:
        update_batch_rate: The rate of the periodic update.
        est_growth_rate: The estimated growth rate of the number of entities.
        min_batch_size: The minimum batch size of the periodic update.
    """

    update_batch_rate: CronExpression = CronExpression(minute="*/10")
    est_growth_rate: float = 0.05
    min_batch_size: int = 10


class Updater:
    """Executes periodic update callbacks."""

    def __init__(
        self,
        db: EntityDatabase,
        platform_config: PlatformConfig,
        scheduler: Scheduler,
    ):
        self.log = logging.getLogger("Updater")
        self.model_spec = platform_config.model_spec
        self.config = UpdaterConfig.model_validate(platform_config.config.get("updater", {}))
        self.db = db
        self.scheduler = scheduler

        self.enabled = platform_config.process_index == 0

        if not self.enabled:
            return

        # Get state cache
        self.cache = self.db.get_module_cache("Updater")
        self._setup_cache_indexes()

    def _setup_cache_indexes(self):
        """TODO: Sets up indexes for the cache collection."""

    @validate_call
    def register_record_update_hook(
        self,
        hook: Callable[[str, str, dict], list[DataPointTask]],
        hook_id: str,
        entity_type: str,
        period: ParsedTimedelta,
    ):
        """Registers a hook for periodic update of entities of the specified type.

        The hook receives the entity type, the entity ID and the master record.
        """

    @validate_call
    def register_eid_update_hook(
        self,
        hook: Callable[[str, str], list[DataPointTask]],
        hook_id: str,
        entity_type: str,
        period: Union[timedelta, str],
    ):
        """Registers a hook for periodic update of entities of the specified type.

        The hook receives the entity type and the entity ID.
        """

    def start(self):
        """
        TODO:
            Resolve configured hooks with cached previous state.
            Register CRON triggers based on module registrations.
            Connect to RabbitMQ to push tasks to.
        """

    def stop(self):
        """
        Stops the updater.

        TODO: Disconnect from RabbitMQ.
        """

    def process_update_batch(self, entity_type, hooks, state):
        """Processes a batch of entities of the specified type.

        Args:
            entity_type: The entity type.
            hooks: The update hooks.
            state: The state of the update process.
        """

    def process_eid_update_batch(self, entity_type, hooks, state):
        """Processes a batch of entities of the specified type, only passing the entity ID.

        Args:
            entity_type: The entity type.
            hooks: The update hooks.
            state: The state of the update process.
        """
