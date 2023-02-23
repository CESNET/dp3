import logging
from time import time

from dp3.common.base_module import BaseModule
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.task import Task


class TestModule(BaseModule):
    def __init__(self, registrar: CallbackRegistrar):
        self.log = logging.getLogger("TestModule")
        self.log.setLevel("DEBUG")

        # just for testing purposes - as new value for test_attrib
        self.counter = 0

        registrar.register_entity_hook(
            "on_entity_creation", hook=self.fill_test_attr_string, entity="test_entity_type"
        )

        registrar.register_correlation_hook(
            hook=self.processing_func_timestamp,
            entity_type="test_entity_type",
            depends_on=[["test_attr_string"]],
            may_change=[["test_attr_time"]],
        )

        registrar.register_correlation_hook(
            hook=self.processing_func_test_attrib,
            entity_type="test_entity_type",
            depends_on=[["test_attr_time"]],
            may_change=[["test_attr_int"]],
        )

    def fill_test_attr_string(self, ekey: str, task: Task) -> list[Task]:
        """receives ekey and Task, may return new Tasks (including new DataPoints)"""
        return [
            Task(
                model_spec=self.model_spec,
                entity_type="test_entity_type",
                ekey=ekey,
                data_points=[
                    {
                        "etype": "test_entity_type",
                        "eid": ekey,
                        "attr": "test_attr_string",
                        "src": "secondary/test_module",
                        "v": f"entity key {ekey}",
                    }
                ],
            )
        ]

    def processing_func_timestamp(self, ekey: str, record: dict):
        """
        Set current time to 'test_timestamp'
        Args:
            ekey: entity identifier
            record: record with current values
        Returns:
            new update as Task.
        """
        print("Hello from TestModule - processing_func_timestamp")
        current_time = time()

        record["test_attr_time"] = current_time

    def processing_func_test_attrib(self, ekey: str, record: dict):
        """
        Increase test_attrib's value by one.
        Args:
            ekey: entity identificator
            record: record with current values
        Returns:
            new update as Task.
        """
        print("Hello from TestModule - processing_func_attrib")
        self.counter += 1

        record["test_attrib"] = self.counter
