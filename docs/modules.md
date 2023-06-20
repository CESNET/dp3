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
The declaration of [`BaseModule`][dp3.common.base_module.BaseModule] is as follows:

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

## Configuration

[`PlatformConfig`][dp3.common.config.PlatformConfig] contains the entire DP³ platform configuration,
which includes the application name, worker counts, which worker processes is the module running in
and a [`ModelSpec`][dp3.common.config.ModelSpec] which contains the entity specification.

If you want to create configuration specific to the module itself, create a `.yml` configuration file 
named as the module itself inside the `modules/` folder,
as described in the [modules configuration page](configuration/modules.md).
This configuration will be then loaded into the `module_config` dictionary for convenience.

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
    self.delete_old_dps, minute="*/10"  # (1)
)
registrar.scheduler_register(
    self.archive_old_dps, minute=0, hour=2  # (2)
)  
```

1. At every 10th minute.
2. Every day at 2 AM.

By default, the callback will receive no arguments, but you can pass static arguments for every call
using the `func_args` and `func_kwargs` keyword arguments. 
The function return value will always be ignored.

The complete documentation can be found at the 
[`scheduler_register`][dp3.common.callback_registrar.CallbackRegistrar.scheduler_register] page.
As DP3 utilizes the [APScheduler](https://apscheduler.readthedocs.io/en/latest/) package internally
to realize this functionality, specifically the [`CronTrigger`](https://apscheduler.readthedocs.io/en/latest/modules/triggers/cron.html), feel free to check their documentation for more details.


### Callbacks within processing

There are a number of possible places to register callback functions during data-point processing.

#### Task `on_task_start` hook

A hook will be called on task processing start.
Required signature is `Callable[[DataPointTask], Any]`, as the return value is ignored.
It may be useful for implementing custom statistics.

Here is an example usage:
```python
def task_hook(task: DataPointTask):
    print(task.etype)

registrar.register_task_hook("on_task_start", task_hook)
```



#### Entity `allow_entity_creation` hook

Receives eid and Task, may prevent entity record creation (by returning False).
Required signature is `Callable[[str, DataPointTask], bool]`.

example:
```python
def entity_creation(eid: str, task: DataPointTask) -> bool:
    return eid.startswith("1")

registrar.register_entity_hook(
    "allow_entity_creation", entity_creation, "test_entity_type"
)
```

#### Entity `on_entity_creation` hook

Receives eid and Task, may return new DataPointTasks.
Required signature is `Callable[[str, DataPointTask], list[DataPointTask]]`.

example:
```python
def processing_function(eid: str, task: DataPointTask) -> list[DataPointTask]:
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

registrar.register_entity_hook(
    "on_entity_creation", processing_function, "test_entity_type"
)
```

#### Attribute hooks

There are register points for all attribute types:
`on_new_plain`, `on_new_observation`, `on_new_ts_chunk`.
The callback allways receives eid, attribute and Task, and may return new DataPointTasks.
The required signature is `Callable[[str, DataPointBase], list[DataPointTask]]`.

Here is an example usage:
```python
def attr_hook(eid: str, dp: DataPointBase) -> list[DataPointTask]:
    ...
    return []

registrar.register_attr_hook(
    "on_new_observation", attr_hook, "test_entity_type", "test_attr_type",
)
```

#### Timeseries hook

Timeseries hooks are run before snapshot creation, and allow to process the accumulated
timeseries data into observations / plain attributes to be accessed in snapshots.
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



## Running module code in a separate thread

...