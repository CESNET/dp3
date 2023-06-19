# Modules

DP³ enables its users to create custom modules to perform application specific data analysis.
Modules are loaded using a plugin-like architecture and can influence the data flow from the
very first moment upon handling the data-point push request.

As described in the [Architecture](architecture.md) page, DP³ uses a categorization of modules
into **primary** and **secondary** modules. 
The distinction between **primary** and **secondary modules** is such that primary modules
send data-points _into the system_ using the HTTP API, while secondary modules _react
to the data present in the system_, e.g.: altering the data-flow in an application-specific manner,
deriving additional data based on incoming data-points or performing data correlation on entity snapshots.

This page covers the DP³ API for secondary modules, 
for primary module implementation, the [API documentation](../api/#insert-datapoints) may be useful, 
also feel free to check out the dummy_sender script in `/scripts/dummy_sender.py`.

## Creating a new Module

All modules must subclass the [`BaseModule`][dp3.common.base_module.BaseModule] class.
If a class does not subclass the [`BaseModule`][dp3.common.base_module.BaseModule] class,
it will not be loaded and activated by the main DP³ worker.
The declaration of `BaseModule` is as follows:

```python
class BaseModule(ABC):

    @abstractmethod
    def __init__(
        self, 
        platform_config: PlatformConfig, 
        module_config: dict, 
        registrar: CallbackRegistrar
    ):
        pass
```

At initialization, each module receives a [`PlatformConfig`][dp3.common.config.PlatformConfig],
a `module_config` dictionary and a 
[`CallbackRegistrar`][dp3.common.callback_registrar.CallbackRegistrar].
Let's go through them one at a time.

### Configuration

[`PlatformConfig`][dp3.common.config.PlatformConfig] contains the entire DP³ platform configuration,
which includes the application name, worker counts, which worker processes is the module running in
and a [`ModelSpec`][dp3.common.config.ModelSpec] which contains the entity specification.

If you want to pass configuration specific to the module itself, create a `.yml` configuration file 
named as the module itself inside the `modules/` folder,
as described in the [modules configuration page](configuration/modules.md).
This configuration will be then loaded into the `module_config` dictionary for convenience.

### Callbacks

The [`CallbackRegistrar`][dp3.common.callback_registrar.CallbackRegistrar]. class provides
the API to register callbacks to be called during the data processing.

#### CRON Trigger Periodic Callbacks

#### Callbacks within processing

#### Correlation callbacks

### Running module code in a separate thread

...