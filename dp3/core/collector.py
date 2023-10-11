"""
Core module performing deletion of entities based on specified policy.
"""
import logging
from datetime import datetime, timedelta
from functools import partial

from pydantic import BaseModel

from dp3.common.attrspec import AttrType
from dp3.common.base_module import BaseModule
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import CronExpression, PlatformConfig
from dp3.common.datapoint import DataPointBase, DataPointObservationsBase, DataPointTimeseriesBase
from dp3.common.task import DataPointTask


class CollectorConfig(BaseModel):
    """The configuration of the Collector module.

    Attributes:
        collection_rate: The rate at which the collector module runs. Default is 3:00 AM every day.
    """

    collection_rate: CronExpression = CronExpression(hour="3", minute="0", second="0")


class Collector(BaseModule):
    """Collector module manages the lifetimes of entities based on specified policy."""

    def __init__(
        self, platform_config: PlatformConfig, module_config: dict, registrar: CallbackRegistrar
    ):
        self.log = logging.getLogger("Collector")
        self.model_spec = platform_config.model_spec

        for entity, entity_config in self.model_spec.entities.items():
            lifetime = entity_config.lifetime
            if lifetime.type == "immortal":
                pass
            elif lifetime.type == "ttl":
                registrar.register_entity_hook(
                    "on_entity_creation",
                    partial(self.extend_ttl_on_create, base_ttl=lifetime.on_create),
                    entity,
                )

                self._register_ttl_extensions(entity, registrar, lifetime.mirror_data)

                # TODO register collection

            elif lifetime.type == "weak":
                pass  # TODO
            else:
                raise ValueError(f"Unknown lifetime type: {lifetime.type}")

        registrar.register_attr_hook(
            "on_new_observation",
            partial(self.extend_observations_ttl, extend_by=timedelta(days=1)),
            "test_entity_type",
            "test_attr_type",
        )

    def extend_ttl_on_create(
        self, eid: str, task: DataPointTask, base_ttl: timedelta
    ) -> list[DataPointTask]:
        """Extends the TTL of the entity by the specified timedelta."""
        task = DataPointTask(
            model_spec=self.model_spec,
            etype=task.etype,
            eid=eid,
            ttl_tokens={"base": datetime.utcnow() + base_ttl},
        )
        return [task]

    def _register_ttl_extensions(
        self, entity: str, registrar: CallbackRegistrar, mirror_data: bool
    ):
        for attr, attr_spec in self.model_spec.entity_attributes[entity].items():
            if attr_spec.t == AttrType.TIMESERIES:
                if mirror_data and attr_spec.timeseries_params.max_age is not None:
                    ttl = max(attr_spec.ttl, attr_spec.timeseries_params.max_age)
                else:
                    ttl = attr_spec.ttl
                if not ttl:
                    continue

                registrar.register_attr_hook(
                    "on_new_timeseries",
                    partial(self.extend_timeseries_ttl, extend_by=ttl),
                    entity,
                    attr,
                )
            elif attr_spec.t == AttrType.OBSERVATIONS:
                if mirror_data and attr_spec.history_params.max_age is not None:
                    ttl = max(attr_spec.ttl, attr_spec.history_params.max_age)
                else:
                    ttl = attr_spec.ttl
                if not ttl:
                    continue

                registrar.register_attr_hook(
                    "on_new_observation",
                    partial(self.extend_observations_ttl, extend_by=ttl),
                    entity,
                    attr,
                )
            elif attr_spec.t == AttrType.PLAIN:
                if not attr_spec.ttl:
                    continue

                registrar.register_attr_hook(
                    "on_new_plain",
                    partial(self.extend_plain_ttl, extend_by=attr_spec.ttl),
                    entity,
                    attr,
                )
            else:
                raise ValueError(f"Unknown attribute type: {attr_spec.t}")

    def extend_plain_ttl(
        self, eid: str, dp: DataPointBase, extend_by: timedelta
    ) -> list[DataPointTask]:
        """Extends the TTL of the entity by the specified timedelta."""
        now = datetime.utcnow()
        task = DataPointTask(
            model_spec=self.model_spec,
            etype=dp.etype,
            eid=eid,
            ttl_tokens={"data": now + extend_by},
        )
        return [task]

    def extend_observations_ttl(
        self, eid: str, dp: DataPointObservationsBase, extend_by: timedelta
    ) -> list[DataPointTask]:
        """Extends the TTL of the entity by the specified timedelta."""
        task = DataPointTask(
            model_spec=self.model_spec,
            etype=dp.etype,
            eid=eid,
            ttl_tokens={"data": dp.t2 + extend_by},
        )
        return [task]

    def extend_timeseries_ttl(
        self, eid: str, dp: DataPointTimeseriesBase, extend_by: timedelta
    ) -> list[DataPointTask]:
        """Extends the TTL of the entity by the specified timedelta."""
        task = DataPointTask(
            model_spec=self.model_spec,
            etype=dp.etype,
            eid=eid,
            ttl_tokens={"data": dp.t2 + extend_by},
        )
        return [task]
