from abc import ABC, abstractmethod

from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import PlatformConfig


class BaseModule(ABC):
    """
    Abstract class for platform modules.
    Every module must inherit this abstract class for automatic loading of module!
    """

    @abstractmethod
    def __init__(
        self, platform_config: PlatformConfig, module_config: dict, registrar: CallbackRegistrar
    ):
        """Initialize the module and register callbacks.
        Args:
            platform_config: Platform configuration class
            module_config: Configuration of the module,
                equivalent of `platform_config.config.get("modules.<module_name>")`
            registrar: A callback / hook registration interface
        """

    def start(self) -> None:
        """
        Run the module - used to run own thread if needed.

        Called after initialization, may be used to create and run a separate
        thread if needed by the module. Do nothing unless overridden.
        """
        return None

    def stop(self) -> None:
        """
        Stop the module - used to stop own thread.

        Called before program exit, may be used to finalize and stop the
        separate thread if it is used. Do nothing unless overridden.
        """
        return None
