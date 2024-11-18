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

1. After calling the `BaseModule` constructor, the module should and register its [callbacks](#callbacks) in the `__init__` method.
2. The `load_config` method is called by the `BaseModule`'s `__init__` method, and is used to load the module-specific configuration. It can be also called during system runtime to reload the configuration.

You should place loading of module's configuration inside the `load_config` method. 
This method is called by the `BaseModule`'s `__init__` method, as you can see bellow.
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
For the module to do anything, it must read the provided configuration from `platform_config`and
`module_config` and register callbacks to perform data analysis using the `registrar` object.
Let's go through them one at a time.

## Configuration

[`PlatformConfig`][dp3.common.config.PlatformConfig] contains the entire DP³ platform configuration,
which includes the application name, worker counts, which worker processes is the module running in
and a [`ModelSpec`][dp3.common.config.ModelSpec] which contains the entity specification.

If you want to create configuration specific to the module itself, create a `.yml` configuration file 
named as the module itself inside the `modules/` folder,
as described in the [modules configuration page](configuration/modules.md).
This configuration will be then loaded into the `module_config` dictionary for convenience.
Please place code that loads the module configuration into the `load_config` method,
where you will recieve both the `platform_config` and `module_config` as arguments.

## Reloading configuration

The module configuration can be reloaded during runtime using the [Control](configuration/control.md) API, specifically the [`refresh_module_config`](configuration/control.md#refresh_module_config) action.
This will cause the `load_config` method to be called again, with the new configuration.
For this reason it is recommended to place all configuration loading code into the `load_config` method.

Some callbacks may be called only sparsely in the lifetime of an entity,
and it may be useful to refresh all the values derived by the module when the configuration changes.
This is implemented for the [`on_entity_creation`](#entity-on_entity_creation-hook) 
and [`on_new_attr`](#attribute-hooks) callbacks, 
and you can enable it by passing the `refresh` keyword argument
to the callback registration. See the Callbacks section for more details.

## Type of `eid`

!!! tip "Specifying the `eid` type"

    At runtime, the `eid` will be exactly the type as specified in the entity specification.

All the examples on this page will show the `eid` as a string, as that is the default type.
The type of the `eid` is can be configured in the entity specification, as is
detailed [here](configuration//db_entities.md#entity).

The typehint of the `eid` used in callback registration definitions is the [
`AnyEidT`][dp3.common.datatype.AnyEidT] type, which is a type alias of Union of all the allowed
types of the `eid` in the entity specification.

## Callbacks

The `registrar:` [`CallbackRegistrar`][dp3.common.callback_registrar.CallbackRegistrar] object
provides the API to register callbacks to be called during the data processing.

### CRON Trigger Periodic Callbacks

For callbacks that need to be called periodically, 
the [`scheduler_register`][dp3.common.callback_registrar.CallbackRegistrar.scheduler_register]
is used. 
The specific times the callback will be called are defined using the CRON schedule expressions.
Here is a simplified example from the HistoryManager module:

```python
registrar.scheduler_register(
    self.delete_old_dps, minute="*/10"  # (1)!
)
registrar.scheduler_register(
    self.archive_old_dps, minute=0, hour=2  # (2)!
)  
```

1. At every 10th minute.
2. Every day at 2 AM.

By default, the callback will receive no arguments, but you can pass static arguments for every call
using the `func_args` and `func_kwargs` keyword arguments. 
The function return value will always be ignored.

The complete documentation can be found at the 
[`scheduler_register`][dp3.common.callback_registrar.CallbackRegistrar.scheduler_register] page.
As DP³ utilizes the [APScheduler](https://apscheduler.readthedocs.io/en/latest/) package internally
to realize this functionality, specifically the [`CronTrigger`](https://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html), feel free to check their documentation for more details.


### Callbacks within processing

There are a number of possible places to register callback functions during data-point processing.

#### Task `on_task_start` hook

A hook will be called on task processing start.
The callback is registered using the 
[`register_task_hook`][dp3.common.callback_registrar.CallbackRegistrar.register_task_hook] method.
Required signature is `Callable[[DataPointTask], Any]`, as the return value is ignored.
It may be useful for implementing custom statistics.

```python
def task_hook(task: DataPointTask):
    print(task.etype)

registrar.register_task_hook("on_task_start", task_hook)
```



#### Entity `allow_entity_creation` hook

Receives eid and Task, may prevent entity record creation (by returning False).
The callback is registered using the 
[`register_allow_entity_creation_hook`][dp3.common.callback_registrar.CallbackRegistrar.register_allow_entity_creation_hook] method.
Required signature is `Callable[[AnyEidT, DataPointTask], bool]`.

```python
def entity_creation(
        eid: str,  # (1)! 
        task: DataPointTask,
) -> bool:
    return eid.startswith("1")

registrar.register_allow_entity_creation_hook(
    entity_creation, "test_entity_type"
)
```

1. `eid` may not be string, depending on the entity configuration, see [Type of
   `eid`](#type-of-eid).

#### Entity `on_entity_creation` hook

Receives eid and Task, may return new DataPointTasks.

Callbacks which are called once when an entity is created are registered using the 
[`register_on_entity_creation_hook`][dp3.common.callback_registrar.CallbackRegistrar.register_on_entity_creation_hook] method.
Required signature is `Callable[[AnyEidT, DataPointTask], list[DataPointTask]]`.

```python
def processing_function(
        eid: str,  # (1)! 
        task: DataPointTask
) -> list[DataPointTask]:
    output = does_work(task)
    return [DataPointTask(
        model_spec=task.model_spec,
        etype="mac",
        eid=eid,
        data_points=[{
            "etype": "test_enitity_type",
            "eid": eid,
            "attr": "derived_on_creation",
            "src": "secondary/derived_on_creation",
            "v": output
        }]
    )]

registrar.register_on_entity_creation_hook(
    processing_function, "test_entity_type"
)
```

1. `eid` may not be string, depending on the entity configuration, see [Type of
   `eid`](#type-of-eid).

The `register_on_entity_creation_hook` method also allows for refreshing of values derived 
by the registered hook. This can be done using the `refresh` keyword argument, (expecting a [`SharedFlag`][dp3.common.state.SharedFlag] object, which is created by default for all modules)
and the `may_change` keyword argument, which lists all the attributes that the hook may change.
For the above example, the registration would look like this:

```python
registrar.register_on_entity_creation_hook(
    processing_function, 
    "test_entity_type", 
    refresh=self.refresh,
    may_change=[["derived_on_creation"]]
)
```

#### Attribute hooks

Callbacks that are called on every incoming datapoint of an attribute are registered using the 
[`register_on_new_attr_hook`][dp3.common.callback_registrar.CallbackRegistrar.register_on_new_attr_hook] method.
The callback allways receives eid, attribute and Task, and may return new DataPointTasks.
The required signature is `Callable[[AnyEidT, DataPointBase], Union[None, list[DataPointTask]]]`.

```python
def attr_hook(
        eid: str,  # (1)!
        dp: DataPointBase,
) -> list[DataPointTask]:
    ...
    return []

registrar.register_on_new_attr_hook(
    attr_hook, "test_entity_type", "test_attr_type",
)
```

1. `eid` may not be string, depending on the entity configuration, see [Type of
   `eid`](#type-of-eid).

This hook can be refreshed on configuration changes if you feel like the attribute value may change too slowly
to catch up naturally. 
This can be done using the `refresh` keyword argument, (expecting a [`SharedFlag`][dp3.common.state.SharedFlag] object, which is created by default for all modules)
and the `may_change` keyword argument, which lists all the attributes that the hook may change.
For the above example, the registration would look like this:

```python
registrar.register_on_new_attr_hook(
    attr_hook, 
    "test_entity_type", 
    "test_attr_type", 
    refresh=self.refresh,
    may_change=[]  # (1)!
)
```

1. If the hook may change the value of any attributes, they must be listed here.

#### Timeseries hook

Timeseries hooks are run before snapshot creation, and allow to process the accumulated
timeseries data into observations / plain attributes to be accessed in snapshots.

Callbacks are registered using the 
[`register_timeseries_hook`][dp3.common.callback_registrar.CallbackRegistrar.register_timeseries_hook] method.
The expected callback signature is `Callable[[str, str, list[dict]], list[DataPointTask]]`,
as the callback should expect entity_type, attr_type and attribute history as arguments 
and return a list of DataPointTask objects.

```python
def timeseries_hook(
        entity_type: str, attr_type: str, attr_history: list[dict]
) -> list[DataPointTask]:
    ...
    return []

registrar.register_timeseries_hook(
    timeseries_hook, "test_entity_type", "test_attr_type",
)
```

### Correlation callbacks

Correlation callbacks are called during snapshot creation, and allow to perform analysis
on the data of the snapshot.

#### Snapshots Correlation Hook

The [`register_correlation_hook`][dp3.common.callback_registrar.CallbackRegistrar.register_correlation_hook]
method expects a callable with the following signature: 
`Callable[[str, dict], Union[None, list[DataPointTask]]]`, where the first argument is the entity type, and the second is a dict
containing the current values of the entity and its linked entities.
The method can optionally return a list of DataPointTask objects to be inserted into the system.

As correlation hooks can depend on each other, the hook inputs and outputs must be specified
using the depends_on and may_change arguments. Both arguments are lists of lists of strings,
where each list of strings is a path from the specified entity type to individual attributes (even on linked entities).
For example, if the entity type is `test_entity_type`, and the hook depends on the attribute `test_attr_type1`,
the path is simply `[["test_attr_type1"]]`. If the hook depends on the attribute `test_attr_type1` 
of an  entity linked using `test_attr_link`, the path will be `[["test_attr_link", "test_attr_type1"]]`.

```python
def correlation_hook(entity_type: str, values: dict):
    ...

registrar.register_correlation_hook(
    correlation_hook, "test_entity_type", [["test_attr_type1"]], [["test_attr_type2"]]
)
```

The order of running callbacks is determined automatically, based on the dependencies.
If there is a cycle in the dependencies, a `ValueError` will be raised at registration.
Also, if the provided dependency / output paths are invalid, a `ValueError` will be raised.

#### Snapshots Init Hook

This hook is called before each snapshot creation run begins.
The use case is to enable your module to perform some initialization before the snapshot creation and associated correlation callbacks are executed.
Your hook will recieve no arguments, and you may return a list of DataPointTask objects to be inserted into the system from this hook.

```python
def snapshot_init_hook() -> list[DataPointTask]:
    ...
    return [] 

registrar.register_snapshot_init_hook(snapshot_init_hook)
```

#### Snapshots Finalize Hook

This hook is called after each snapshot creation run ends.
The use case is to enable your module to finish up after the snapshot creation and associated correlation callbacks are executed.
Your hook will recieve no arguments, and you may again return a list of DataPointTask objects to be inserted into the system from this hook.

```python
def snapshot_finalize_hook() -> list[DataPointTask]:
    ...
    return [] 

registrar.register_snapshot_finalize_hook(snapshot_init_hook)
```

### Periodic Update Callbacks

Snapshots, which are designed to execute as quickly as possible parallelized over all workers,
may not fit every use-case for updates, especially when the updates are not tied only to the entity's state.
For example, fetching data from an external system where rate limits are a concern.

This is where the updater module comes in.
The updater module is responsible for updating all entities in the database over a longer time frame.
Learn more about the updater module in the [updater configuration](configuration/updater.md) page.

#### Periodic Update Hook

The [`register_periodic_update_hook`][dp3.common.callback_registrar.CallbackRegistrar.register_periodic_update_hook]
method expects a callable with the following signature:
`Callable[[str, AnyEidT, dict], list[DataPointTask]]`, where the arguments are the entity type,
entity ID and master record. 
The callable should return a list of DataPointTask objects to perform (possibly empty).

You must also pass a unique `hook_id` string when registering the hook, the entity type and 
the period over which the hook should be called for all entities.
The following example shows how to register a periodic update hook for an entity type `test_entity_type`.
The hook will be called for all entities of this type every day.

```python
def periodic_update_hook(
        entity_type: str,
        eid: str,  # (1)!
        record: dict,
) -> list[DataPointTask]:
    ...
    return []

registrar.register_periodic_update_hook(
    periodic_update_hook, "test_id", "test_entity_type", "1d"
)
```

1. `eid` may not be string, depending on the entity configuration, see [Type of
   `eid`](#type-of-eid).

!!! warning "Set a Realistic Update Period"

    Try to configure the period to match the real execution time of the registered hooks, 
    as when the period is too short, the hooks may not finish before the next batch is run,
    leading to missed runs and potentially even to doubling of the effective update period.


#### Periodic Update EID Hook

The periodic eid update hook is similar to the previous periodic update hook, but the entity record is not passed to the callback.
This hook is useful when the entity record is not needed for the update, meaning the record data does not have to be fetched from the database.

The [`register_periodic_eid_update_hook`][dp3.common.callback_registrar.CallbackRegistrar.register_periodic_eid_update_hook]
method expects a callable with the following signature:
`Callable[[str, AnyEidT], list[DataPointTask]]`, where the first argument is the entity type and the second is the entity ID.
The callable should return a list of DataPointTask objects to perform (possibly empty).
All other arguments are the same as for the [periodic update hook](#periodic-update-hook).

```python
def periodic_eid_update_hook(
        entity_type: str,
        eid: str,  # (1)!
) -> list[DataPointTask]:
    ...
    return []

registrar.register_periodic_eid_update_hook(
    periodic_eid_update_hook, "test_id", "test_entity_type", "1d"
)
```

1. `eid` may not be string, depending on the entity configuration, see [Type of
   `eid`](#type-of-eid).

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