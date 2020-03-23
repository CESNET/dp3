class BaseModule:
    """
    Abstract class for platform modules. Every module must inherit this abstract class for automatic loading of module!
    """
    def start(self):
        """
        Run the module - used to run own thread if needed.

        Called after initialization, may be used to create and run a separate
        thread if needed by the module. Do nothing unless overridden.
        """
        pass

    def stop(self):
        """
        Stop the module - used to stop own thread.

        Called before program exit, may be used to finalize and stop the
        separate thread if it is used. Do nothing unless overridden.
        """
        pass
