"""unittest base class for DP3 secondary module tests."""

import copy
import unittest
from collections.abc import Iterable
from datetime import datetime
from typing import Any, Callable, Generic, Optional, TypeVar, Union

from dp3.common.base_module import BaseModule
from dp3.common.config import HierarchicalDict, ModelSpec, PlatformConfig
from dp3.common.datapoint import DataPointBase
from dp3.common.task import DataPointTask, task_context
from dp3.common.types import UTC
from dp3.common.utils import get_func_name
from dp3.testing.assertions import ModuleAssertions
from dp3.testing.config import (
    CONFIG_DIR_ENV,
    build_model_spec,
    build_platform_config,
    get_module_config,
    load_config,
    resolve_config_dir,
)
from dp3.testing.registrar import HookRegistration, TestCallbackRegistrar

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

    def run_scheduler_job(self, job: Union[int, str, Callable, HookRegistration]):
        return self.registrar.run_scheduler_job(job)

    def registered(self, kind: Optional[str] = None, **fields) -> list[HookRegistration]:
        """Return registrations matching ``kind`` and the supplied registration fields."""
        return [
            registration
            for registration in self.registrar.registrations
            if self._registration_matches(registration, kind, fields)
        ]

    def assert_registered(self, kind: str, **fields) -> HookRegistration:
        """Assert that at least one callback registration matches the supplied fields."""
        matches = self.registered(kind, **fields)
        if not matches:
            self.fail(
                f"No registration matched kind={kind!r}, fields={fields!r}. "
                f"Registered callbacks: {self.registrar.registrations!r}"
            )
        return matches[0]

    def assert_registered_once(self, kind: str, **fields) -> HookRegistration:
        """Assert that exactly one callback registration matches the supplied fields."""
        matches = self.registered(kind, **fields)
        if len(matches) != 1:
            self.fail(
                f"Expected one registration matching kind={kind!r}, fields={fields!r}; "
                f"found {len(matches)}: {matches!r}"
            )
        return matches[0]

    def assert_registered_attrs(
        self,
        entity: str,
        expected_attrs: Iterable[str],
        *,
        kind: str = "on_new_attr",
        exact: bool = True,
    ) -> list[HookRegistration]:
        """Assert that attribute hook registrations exist for the supplied entity attributes."""
        expected = set(expected_attrs)
        matches = self.registered(kind, entity=entity)
        actual = {registration.attr for registration in matches if registration.attr is not None}
        if exact:
            self.assertEqual(expected, actual)
        else:
            missing = expected - actual
            if missing:
                self.fail(f"Missing registrations for attributes: {sorted(missing)!r}")
        return [registration for registration in matches if registration.attr in expected]

    def assert_scheduler_registered(self, **fields) -> HookRegistration:
        """Assert that at least one scheduler callback registration matches the supplied fields."""
        return self.assert_registered("scheduler", **fields)

    assertRegistered = assert_registered
    assertRegisteredOnce = assert_registered_once
    assertRegisteredAttrs = assert_registered_attrs
    assertSchedulerRegistered = assert_scheduler_registered

    def _registration_matches(
        self, registration: HookRegistration, kind: Optional[str], fields: dict[str, Any]
    ) -> bool:
        if kind is not None and registration.kind != kind:
            return False
        for key, expected in fields.items():
            found, actual = _registration_field(registration, key)
            if not found:
                return False
            if key in {"hook", "func"} and isinstance(expected, str):
                if not _callable_name_matches(actual, expected):
                    return False
            elif not self._partial_match(actual, expected):
                return False
        return True


def _registration_field(registration: HookRegistration, key: str) -> tuple[bool, Any]:
    if key == "func":
        return True, registration.hook
    if hasattr(registration, key):
        return True, getattr(registration, key)
    if key in registration.extra:
        return True, registration.extra[key]
    schedule = registration.extra.get("schedule", {})
    if key in schedule:
        return True, schedule[key]
    return False, None


def _callable_name_matches(func: Callable, expected: str) -> bool:
    func_name = get_func_name(func)
    return func_name == expected or func_name.endswith(f".{expected}")
