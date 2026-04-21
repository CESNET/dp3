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

If you are integrating a module into a DP³ application, start with the task guides [How to add a secondary module](howto/add-module.md) and [How to add an attribute](howto/add-attribute.md), then return here for the reference details.

## Creating a new Module

First, make a directory that will contain all modules of the application.
For example, let's assume that the directory will be called `/modules/`.

As mentioned in the [Processing core configuration](configuration/processing_core.md) page,
the modules directory must be specified in the `modules_dir` configuration option.
Let's create the main module file now - assuming the module will be called `my_awesome_module`,
create a file `/modules/my_awesome_module.py`. 

Finally, to make the processing core load the module, add the module name to the `enabled_modules`
configuration option, e.g.:

```yaml title="Enabling the module in processing_core.yml"
modules_dir: "/modules/"
enabled_modules:
  - "my_awesome_module"
```

Here is a basic skeleton for the module file:

```python
from dp3.common.base_module import BaseModule
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import PlatformConfig

class MyAwesomeModule(BaseModule):
    def __init__(self,
        platform_config: PlatformConfig, 
        module_config: dict, 
        registrar: CallbackRegistrar
    ):
        super().__init__(platform_config, module_config, registrar)
        # (1)!

    def load_config(self, config: PlatformConfig, module_config: dict) -> None:
        ... # (2)!
```

1. After calling the `BaseModule` constructor, the module should register its [callbacks](#callbacks) in the `__init__` method.
2. The `load_config` method is called by the `BaseModule`'s `__init__` method, and is used to load the module-specific configuration. It can be also called during system runtime to reload the configuration.

You should place loading of module's configuration inside the `load_config` method. 
This method is called by the `BaseModule`'s `__init__` method, as you can see below.
It can be also called during system runtime to reload the configuration using the [Control](configuration/control.md) API, 
which enables you to change the module configuration without restarting the DP³ worker.

The registering of callbacks should be done in the `__init__` method however, 
as there is currently no support to alter a callback once it has been registered without restarting.

All modules must subclass the [`BaseModule`][dp3.common.base_module.BaseModule] class.
If a class does not subclass the [`BaseModule`][dp3.common.base_module.BaseModule] class,
it will not be loaded and activated by the main DP³ worker.
The declaration of [`BaseModule`][dp3.common.base_module.BaseModule] is as follows:

```python
class BaseModule:
    def __init__(
        self, 
        platform_config: PlatformConfig,
        module_config: dict,
        registrar: CallbackRegistrar
    ):
        self.refresh: SharedFlag = SharedFlag(
            False, banner=f"Refresh {self.__class__.__name__}"
        )
        self.log: logging.Logger = logging.getLogger(self.__class__.__name__)

        self.load_config(platform_config, module_config)
```

As you can see, the `BaseModule` class provides a `refresh` flag used internally for refreshing
and a `log` object for the module to use for logging. 

At initialization, each module receives a [`PlatformConfig`][dp3.common.config.PlatformConfig],
a `module_config` dictionary and a 
[`CallbackRegistrar`][dp3.common.callback_registrar.CallbackRegistrar].
For the module to do anything, it must read the provided configuration from `platform_config` and
`module_config` and register callbacks to perform data analysis using the `registrar` object.
Let's go through them one at a time.

## Configuration

[`PlatformConfig`][dp3.common.config.PlatformConfig] contains the entire DP³ platform configuration,
which includes the application name, worker counts, which worker processes the module is running in,
and a [`ModelSpec`][dp3.common.config.ModelSpec] which contains the entity specification.

If you want to create configuration specific to the module itself, create a `.yml` configuration file 
named as the module itself inside the `modules/` folder,
as described in the [modules configuration page](configuration/modules.md).
This configuration will be then loaded into the `module_config` dictionary for convenience.
Please place code that loads the module configuration into the `load_config` method,
where you will receive both the `platform_config` and `module_config` as arguments.

## Reloading configuration

The module configuration can be reloaded during runtime using the [Control](configuration/control.md) API, specifically the [`refresh_module_config`](configuration/control.md#refresh_module_config) action.
This will cause the `load_config` method to be called again, with the new configuration.
For this reason it is recommended to place all configuration loading code into the `load_config` method.

Some callbacks may be called only sparsely in the lifetime of an entity,
and it may be useful to refresh all the values derived by the module when the configuration changes.
This is implemented for the [`on_entity_creation`](hooks.md#on_entity_creation) 
and [`on_new_attr`](hooks.md#on_new_attr) callbacks, 
and you can enable it by passing the `refresh` keyword argument
to the callback registration. See the [refresh behavior reference](hooks.md#refresh-on-config-change-behavior-for-ingestion-hooks) for details.

## Callbacks

The `registrar:` [`CallbackRegistrar`][dp3.common.callback_registrar.CallbackRegistrar] object
provides the API to register callbacks during data processing.

The in depth hook guide is the [Module hook reference](hooks.md). Use that page
for lifecycle timing, callback inputs, return-value behavior, refresh behavior, the way
returned `DataPointTask` objects re-enter DP³ processing. What follows is a light index of that page.

### Ingestion-time hooks

- [`register_task_hook("on_task_start", ...)`](hooks.md#on_task_start) — observe every
  incoming `DataPointTask`; the return value is ignored.
- [`register_allow_entity_creation_hook(...)`](hooks.md#allow_entity_creation) — allow
  or deny creation of a new entity.
- [`register_on_entity_creation_hook(...)`](hooks.md#on_entity_creation) — react once
  when an entity is first created.
- [`register_on_new_attr_hook(...)`](hooks.md#on_new_attr) — react to each incoming
  datapoint of a selected attribute.

### Snapshot-time hooks

- [`register_snapshot_init_hook(...)`](hooks.md#snapshot_init) — run whole-run setup
  before snapshot processing begins.
- [`register_timeseries_hook(...)`](hooks.md#timeseries_hook) — process accumulated
  timeseries history before snapshot current values are finalized.
- [`register_correlation_hook(...)`](hooks.md#register_correlation_hook) — reason over
  snapshot-time current values.
- [`register_correlation_hook_with_master_record(...)`](hooks.md#register_correlation_hook_with_master_record)
  — correlation hook variant that also receives the raw `master_record`.
- [`register_snapshot_finalize_hook(...)`](hooks.md#snapshot_finalize) — run whole-run
  teardown after snapshot processing ends.

### Periodic updater hooks

Updater hooks revisit stored entities over a configured period. For the updater scheduling model
and configuration, see the [updater configuration](configuration/updater.md) page.

- [`register_periodic_update_hook(...)`](hooks.md#periodic_update_hook) — periodic
  sweep with `master_record`.
- [`register_periodic_eid_update_hook(...)`](hooks.md#periodic_eid_update_hook) —
  lighter periodic sweep when only the entity identity is needed.

### Scheduled callbacks

- [`scheduler_register(...)`](hooks.md#scheduler_register) — CRON-style module-level
  scheduled callback for maintenance, polling, housekeeping, or shared-state reloads.

## Running module code in a separate thread

The module is free to run its own code in separate threads or processes.
To synchronize such code with the platform, use the `start()` and `stop()`
methods of the `BaseModule` class.
the `start()` method is called after the platform is initialized, and the `stop()` method
is called before the platform is shut down.

```python
class MyModule(BaseModule):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._thread = None
        self._stop_event = threading.Event()
        self.log = logging.getLogger("MyModule")

    def start(self):
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop_event.set()
        self._thread.join()

    def _run(self):
        while not self._stop_event.is_set():
            self.log.info("Hello world!")
            time.sleep(1)
```