import logging

from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import PlatformConfig
from dp3.common.state import SharedFlag


class BaseModule:
    """
    Base class for platform modules.
    Every module must inherit this abstract class for automatic loading of module!

    Attributes:
        refresh: SharedFlag that is set to True when module should refresh its attributes values
        log: Logger for the module
    """

    def __init__(
        self, platform_config: PlatformConfig, module_config: dict, registrar: CallbackRegistrar
    ):
        """Initialize the module and register callbacks.

        `self.load_config()` is called in the base class.

        Args:
            platform_config: Platform configuration class
            module_config: Configuration of the module,
                equivalent of `platform_config.config.get("modules.<module_name>")`
            registrar: A callback / hook registration interface
        """
        self.refresh: SharedFlag = SharedFlag(False, banner=f"Refresh {self.__class__.__name__}")
        self.log: logging.Logger = logging.getLogger(self.__class__.__name__)

        self.load_config(platform_config, module_config)

    def load_config(self, config: PlatformConfig, module_config: dict) -> None:
        """Load module configuration.

        Is called on module initialization and on refresh request via `/control` API.
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
