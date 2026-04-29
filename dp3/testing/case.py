"""unittest base class for DP3 secondary module tests."""

import copy
import unittest
from datetime import datetime
from typing import Any, Generic, Optional, TypeVar, Union

from dp3.common.base_module import BaseModule
from dp3.common.config import HierarchicalDict, ModelSpec, PlatformConfig
from dp3.common.datapoint import DataPointBase
from dp3.common.task import DataPointTask, task_context
from dp3.common.types import UTC
from dp3.testing.assertions import ModuleAssertions
from dp3.testing.config import (
    CONFIG_DIR_ENV,
    build_model_spec,
    build_platform_config,
    get_module_config,
    load_config,
    resolve_config_dir,
)
from dp3.testing.registrar import TestCallbackRegistrar

ModuleT = TypeVar("ModuleT", bound=BaseModule)


class DP3ModuleTestCase(ModuleAssertions, unittest.TestCase, Generic[ModuleT]):
    """Base class for unit tests of DP3 secondary modules.

    By default the app configuration directory is read from ``DP3_CONFIG_DIR``. Subclasses may set
    ``config_dir`` explicitly when they need a fixed fixture config.
    """

    config_dir: Optional[str] = None
    config_env_var: str = CONFIG_DIR_ENV
    module_class: type[ModuleT]
    module_name: Optional[str] = None
    module_config: Optional[dict] = None
    app_name: str = "test"
    process_index: int = 0
    num_processes: int = 1
    module: ModuleT

    def setUp(self) -> None:
        super().setUp()
        self.config_base_path = self.resolve_config_dir()
        self.config = self.load_config()
        self.model_spec = self.make_model_spec(self.config)
        self.platform_config = self.make_platform_config()
        self.registrar = self.make_registrar()
        self.module = self.make_module(self.module_class, self.get_module_config(), self.registrar)

    def resolve_config_dir(self) -> str:
        return resolve_config_dir(self.config_dir, self.config_env_var)

    def load_config(self) -> HierarchicalDict:
        return load_config(self.config_base_path, self.config_env_var)

    def make_model_spec(self, config: HierarchicalDict) -> ModelSpec:
        return build_model_spec(config)

    def make_platform_config(self) -> PlatformConfig:
        return build_platform_config(
            self.config,
            self.model_spec,
            self.config_base_path,
            app_name=self.app_name,
            process_index=self.process_index,
            num_processes=self.num_processes,
            env_var=self.config_env_var,
        )

    def get_module_config(self) -> dict:
        if self.module_config is not None:
            return copy.deepcopy(self.module_config)
        return copy.deepcopy(get_module_config(self.config, self.get_module_name()))

    def get_module_name(self) -> Optional[str]:
        if self.module_name is not None:
            return self.module_name
        return self.module_class.__module__.split(".")[-1]

    def make_registrar(self) -> TestCallbackRegistrar:
        return TestCallbackRegistrar(self.model_spec)

    def make_module(
        self,
        module_class: type[ModuleT],
        module_config: dict[str, Any],
        registrar: TestCallbackRegistrar,
    ) -> ModuleT:
        return module_class(self.platform_config, module_config, registrar)

    def make_task(
        self,
        etype: str,
        eid: Any,
        data_points: Optional[list[Union[dict, DataPointBase]]] = None,
        tags: Optional[list] = None,
        ttl_tokens: Optional[dict] = None,
        delete: bool = False,
    ) -> DataPointTask:
        with task_context(self.model_spec):
            return DataPointTask(
                etype=etype,
                eid=eid,
                data_points=data_points or [],
                tags=tags or [],
                ttl_tokens=ttl_tokens,
                delete=delete,
            )

    def make_datapoint(
        self,
        etype: str,
        eid: Any,
        attr: str,
        v: Any,
        src: str = "test",
        **fields,
    ) -> DataPointBase:
        task = self.make_task(
            etype,
            eid,
            [dict({"etype": etype, "eid": eid, "attr": attr, "src": src, "v": v}, **fields)],
        )
        return task.data_points[0]

    def make_plain_datapoint(
        self, etype: str, eid: Any, attr: str, v: Any, src: str = "test", **fields
    ) -> DataPointBase:
        return self.make_datapoint(etype, eid, attr, v, src=src, **fields)

    def make_observation_datapoint(
        self,
        etype: str,
        eid: Any,
        attr: str,
        v: Any,
        src: str = "test",
        t1: Optional[datetime] = None,
        t2: Optional[datetime] = None,
        c: float = 1.0,
        **fields,
    ) -> DataPointBase:
        t1 = t1 or datetime.now(UTC)
        data = {"t1": t1, "c": c, **fields}
        if t2 is not None:
            data["t2"] = t2
        return self.make_datapoint(etype, eid, attr, v, src=src, **data)

    def run_task_hooks(self, hook_type: str, task: DataPointTask) -> None:
        self.registrar.run_task_hooks(hook_type, task)

    def run_allow_entity_creation(
        self, entity: str, eid: Any, task: Optional[DataPointTask] = None
    ) -> bool:
        task = task or self.make_task(entity, eid)
        return self.registrar.run_allow_entity_creation(entity, eid, task)

    def run_on_entity_creation(
        self, entity: str, eid: Any, task: Optional[DataPointTask] = None
    ) -> list[DataPointTask]:
        task = task or self.make_task(entity, eid)
        return self.registrar.run_on_entity_creation(entity, eid, task)

    def run_on_new_attr(self, entity: str, attr: str, eid: Any, dp: DataPointBase):
        return self.registrar.run_on_new_attr(entity, attr, eid, dp)

    def run_correlation_hooks(
        self,
        entity_type: str,
        record: dict,
        master_record: Optional[dict] = None,
    ) -> list[DataPointTask]:
        return self.registrar.run_correlation_hooks(entity_type, record, master_record)

    def run_periodic_update(
        self, entity_type: str, eid: Any, master_record: dict, hook_id: Optional[str] = None
    ) -> list[DataPointTask]:
        return self.registrar.run_periodic_update(entity_type, eid, master_record, hook_id)

    def run_periodic_eid_update(
        self, entity_type: str, eid: Any, hook_id: Optional[str] = None
    ) -> list[DataPointTask]:
        return self.registrar.run_periodic_eid_update(entity_type, eid, hook_id)
