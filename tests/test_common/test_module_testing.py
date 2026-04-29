import os
import warnings

from dp3.common.base_module import BaseModule
from dp3.common.task import DataPointTask
from dp3.testing import DP3ModuleTestCase


class SampleModule(BaseModule):
    def __init__(self, platform_config, module_config, registrar):
        super().__init__(platform_config, module_config, registrar)
        registrar.register_allow_entity_creation_hook(self.allow_create, "test_entity_type")
        registrar.register_on_entity_creation_hook(self.on_create, "test_entity_type")
        registrar.register_on_new_attr_hook(self.on_string, "test_entity_type", "test_attr_string")
        registrar.register_correlation_hook(
            self.copy_string,
            "test_entity_type",
            depends_on=[["test_attr_string"]],
            may_change=[["test_attr_int"]],
        )
        registrar.register_correlation_hook_with_master_record(
            self.copy_master_float,
            "test_entity_type",
            depends_on=[],
            may_change=[["test_attr_float"]],
        )
        registrar.register_periodic_update_hook(
            self.periodic_record, "sample", "test_entity_type", "1d"
        )

    def load_config(self, _config, module_config):
        self.prefix = module_config.get("prefix", "created")

    def allow_create(self, eid, _task):
        return eid != "deny"

    def on_create(self, eid, _task):
        return [
            DataPointTask(
                etype="test_entity_type",
                eid=eid,
                data_points=[
                    {
                        "etype": "test_entity_type",
                        "eid": eid,
                        "attr": "test_attr_string",
                        "src": "secondary/sample",
                        "v": f"{self.prefix}:{eid}",
                    }
                ],
            )
        ]

    def on_string(self, eid, dp):
        return [
            DataPointTask(
                etype="test_entity_type",
                eid=eid,
                data_points=[
                    {
                        "etype": "test_entity_type",
                        "eid": eid,
                        "attr": "test_attr_int",
                        "src": "secondary/sample",
                        "v": len(dp.v),
                    }
                ],
            )
        ]

    def copy_string(self, _entity_type, record):
        record["test_attr_int"] = len(record["test_attr_string"])

    def copy_master_float(self, _entity_type, record, master_record):
        record["test_attr_float"] = master_record["test_attr_float"]["v"]

    def periodic_record(self, entity_type, eid, master_record):
        return [
            DataPointTask(
                etype=entity_type,
                eid=eid,
                data_points=[
                    {
                        "etype": entity_type,
                        "eid": eid,
                        "attr": "test_attr_string",
                        "src": "secondary/sample",
                        "v": master_record["test_attr_string"]["v"],
                    }
                ],
            )
        ]


class DeprecatedHookModule(BaseModule):
    def __init__(self, platform_config, module_config, registrar):
        super().__init__(platform_config, module_config, registrar)
        registrar.register_entity_hook("on_entity_creation", self.on_create, "test_entity_type")
        registrar.register_attr_hook(
            "on_new_plain", self.on_string, "test_entity_type", "test_attr_string"
        )

    def on_create(self, _eid, _task):
        return []

    def on_string(self, _eid, _dp):
        return []


TEST_CONFIG_DIR = os.path.join(os.path.dirname(__file__), "..", "test_config")


class TestDP3ModuleTestCase(DP3ModuleTestCase):
    config_dir = TEST_CONFIG_DIR
    module_class = SampleModule
    module_config = {"prefix": "hello"}

    def test_entity_creation_hook_and_partial_datapoint_assertion(self):
        self.assertTrue(self.run_allow_entity_creation("test_entity_type", "ok"))
        self.assertFalse(self.run_allow_entity_creation("test_entity_type", "deny"))

        tasks = self.run_on_entity_creation("test_entity_type", "ok")

        self.assertDatapoint(
            tasks,
            etype="test_entity_type",
            eid="ok",
            attr="test_attr_string",
            v="hello:ok",
        )

    def test_attribute_hook(self):
        dp = self.make_plain_datapoint("test_entity_type", "e1", "test_attr_string", "abcd")

        tasks = self.run_on_new_attr("test_entity_type", "test_attr_string", "e1", dp)

        self.assertDatapoint(tasks, attr="test_attr_int", v=4)

    def test_correlation_and_master_record_hooks(self):
        record = {"eid": "e1", "test_attr_string": "abcdef"}
        master_record = {"test_attr_float": {"v": 1.5}}

        tasks = self.run_correlation_hooks("test_entity_type", record, master_record)

        self.assertNoTasks(tasks)
        self.assertRecordContains(record, test_attr_int=6, test_attr_float=1.5)

    def test_periodic_update_hook(self):
        tasks = self.run_periodic_update(
            "test_entity_type",
            "e1",
            {"test_attr_string": {"v": "periodic"}},
            hook_id="sample",
        )

        self.assertDatapoint(tasks, attr="test_attr_string", v="periodic")

    def test_deprecated_registrar_methods_are_supported_with_warnings(self):
        registrar = self.make_registrar()
        with warnings.catch_warnings(record=True) as captured:
            warnings.simplefilter("always", DeprecationWarning)
            self.make_module(DeprecatedHookModule, {}, registrar)

        self.assertEqual(2, len(captured))
        self.assertTrue(all(item.category is DeprecationWarning for item in captured))
        self.assertEqual(2, len(registrar.registrations))
        self.assertTrue(all(reg.deprecated for reg in registrar.registrations))
