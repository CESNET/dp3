from time import time

from dp3.common.base_module import BaseModule
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import PlatformConfig
from dp3.common.task import DataPointTask


class TestModule(BaseModule):
    def __init__(
        self, platform_config: PlatformConfig, module_config: dict, registrar: CallbackRegistrar
    ):
        super().__init__(platform_config, module_config, registrar)

        registrar.scheduler_register(lambda: self.log.info(self.msg), second="*/10")

        registrar.register_entity_hook(
            "on_entity_creation", hook=self.fill_string_attr, entity="example"
        )

        registrar.register_correlation_hook(
            hook=self.processing_func_timestamp,
            entity_type="example",
            depends_on=[["string_attr"]],
            may_change=[["time_attr"]],
        )

        registrar.register_correlation_hook(
            hook=self.processing_func_test_attrib,
            entity_type="example",
            depends_on=[["time_attr"]],
            may_change=[["int_attr"]],
        )

    def load_config(self, config: PlatformConfig, module_config: dict) -> None:
        # just for testing purposes - as new value for test_attrib
        self.counter = module_config.get("init_value", 0)
        self.msg = module_config.get("msg", "Hello World!")

    def fill_string_attr(self, eid: str, task: DataPointTask) -> list[DataPointTask]:
        """receives eid and Task, may return new Tasks (including new DataPoints)"""
        return [
            DataPointTask(
                etype="example",
                eid=eid,
                data_points=[
                    {
                        "etype": "example",
                        "eid": eid,
                        "attr": "string_attr",
                        "src": "secondary/test_module",
                        "v": f"entity key {eid}",
                    }
                ],
            )
        ]

    def processing_func_timestamp(self, eid: str, record: dict):
        """
        Set current time to 'test_timestamp'
        Args:
            eid: entity identifier
            record: record with current values
        Returns:
            new update as Task.
        """
        self.log.info("Hello from TestModule - processing_func_timestamp")
        current_time = time()

        record["time_attr"] = current_time

    def processing_func_test_attrib(self, eid: str, record: dict):
        """
        Increase test_attrib's value by one.
        Args:
            eid: entity identificator
            record: record with current values
        Returns:
            new update as Task.
        """
        self.log.info("Hello from TestModule - processing_func_attrib")
        self.counter += 1

        record["test_attrib"] = self.counter
