import logging
from functools import partial, update_wrapper

from dp3.common.base_module import BaseModule
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import PlatformConfig


def copy_linked(_: str, record: dict, link: str, attr: str):
    record[attr] = record.get(link, {}).get("record", {}).get(attr, None)


def modify_value(_: str, record: dict, attr: str, value):
    record[attr] = value


dummy_hook_abc = update_wrapper(partial(modify_value, attr="data2", value="abc"), modify_value)
dummy_hook_def = update_wrapper(partial(modify_value, attr="data1", value="def"), modify_value)


class TestModule(BaseModule):
    def __init__(
        self, platform_config: PlatformConfig, module_config: dict, registrar: CallbackRegistrar
    ):
        self.log = logging.getLogger("TestModule")
        self.log.setLevel("DEBUG")
        self.model_spec = platform_config.model_spec

        # just for testing purposes - as new value for test_attrib
        self.counter = module_config.get("init_value", 0)

        registrar.register_correlation_hook(
            update_wrapper(partial(copy_linked, link="bs", attr="data1"), copy_linked),
            "A",
            depends_on=[["bs", "data1"]],
            may_change=[["data1"]],
        )
        registrar.register_correlation_hook(
            update_wrapper(partial(copy_linked, link="as", attr="data2"), copy_linked),
            "B",
            depends_on=[["as", "data2"]],
            may_change=[["data2"]],
        )

        # Testing hook dependencies
        registrar.register_correlation_hook(
            update_wrapper(partial(copy_linked, link="ds", attr="data1"), copy_linked),
            "C",
            depends_on=[["ds", "data1"]],
            may_change=[["data1"]],
        )
        registrar.register_correlation_hook(
            update_wrapper(partial(copy_linked, link="cs", attr="data2"), copy_linked),
            "D",
            depends_on=[["cs", "data2"]],
            may_change=[["data2"]],
        )
        registrar.register_correlation_hook(
            update_wrapper(partial(modify_value, attr="data2", value="modifc"), modify_value),
            "C",
            depends_on=[],
            may_change=[["data2"]],
        )
        registrar.register_correlation_hook(
            update_wrapper(partial(modify_value, attr="data1", value="modifd"), modify_value),
            "D",
            depends_on=[],
            may_change=[["data1"]],
        )
