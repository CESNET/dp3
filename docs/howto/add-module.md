# How to add a secondary module

This guide walks through adding a new secondary DP³ module.

A secondary module runs inside the DP³ worker process and reacts to datapoints or stored data that are already in the system. Typical use cases are enrichment, derivation of additional attributes, correlation during snapshots, and periodic recomputation over stored entities.

This guide focuses on integrating the module into an existing DP³ application. It assumes you already know what the module should do.

If you do not already have a local DP³ application running for development, start with [Get started with local DP³ app development](get-started.md).

## 1. Decide what should trigger the module

Before writing any code, decide which DP³ callback matches the behavior you want:

- **Ingestion hooks** for immediate reaction to incoming datapoints
- **Snapshot hooks** for reasoning over snapshot-time current state
- **Updater hooks** for periodic revisits of stored entities
- **Scheduled callbacks** for CRON-like maintenance or polling

Use the [Module Hook Reference](../hooks.md) to pick the right callback. In your module, you can combine as many callbacks as needed for your use-case.

If the module will emit a new attribute, define that attribute first using [How to add an attribute](add-attribute.md).

## 2. Create the module file

Place the module in the application's modules directory, for example:

```text
modules/normalize_hostname.py
```

A small ingestion-time example that reacts to an incoming `hostname` attribute and emits a derived plain attribute:

```python title="modules/normalize_hostname.py"
from dp3.common.base_module import BaseModule
from dp3.common.callback_registrar import CallbackRegistrar
from dp3.common.config import PlatformConfig
from dp3.common.task import DataPointTask
from dp3.common.datapoint import DataPointBase


class NormalizeHostnameModule(BaseModule):
    def __init__(
        self, platform_config: PlatformConfig, module_config: dict, registrar: CallbackRegistrar
    ):
        super().__init__(platform_config, module_config, registrar)

        registrar.register_on_new_attr_hook(
            self.on_hostname,
            entity="device",
            attr="hostname",
        )

    def load_config(self, config: PlatformConfig, module_config: dict) -> None:
        self.target_attr = module_config.get("target_attr", "hostname_normalized")

    def on_hostname(self, eid: str, dp: DataPointBase) -> list[DataPointTask]:
        value = dp.v.strip().lower()
        return [
            DataPointTask(
                etype="device",
                eid=eid,
                data_points=[
                    {
                        "etype": "device",
                        "eid": eid,
                        "attr": self.target_attr,
                        "src": "secondary/normalize_hostname",
                        "v": value,
                    }
                ],
            )
        ]
```

This example is intentionally small. For more hook patterns, see [Modules](../modules.md) and [Hooks](../hooks.md).

## 3. Add optional module-specific configuration

If the module needs configuration beyond the global DP³ config, create `config/modules/<module_name>.yml`.

For the example above:

```yaml title="config/modules/normalize_hostname.yml"
target_attr: hostname_normalized
```

DP³ loads this file into the module's `module_config` argument.

## 4. Enable the module in `processing_core.yml`

The worker only loads modules listed in `enabled_modules`.

```yaml title="config/processing_core.yml"
modules_dir: "../modules"
enabled_modules:
  - "normalize_hostname"
```

The module name is the Python filename without the `.py` extension.

## 5. Reload the worker processes

The new module is picked up by workers at startup. Restart every worker process that should load it.

If you also changed `db_entities` because the module emits a new attribute, reload the API too by following [How to add an attribute](add-attribute.md).

=== "Local shell"

    Restart each worker process that uses the configuration:

    ```shell
    dp3 worker my_app config 0
    ```

=== "Docker Compose app"

    Recreate the application containers:

    ```shell
    docker compose -f docker-compose.app.yml up -d --build
    docker compose -f docker-compose.app.yml ps
    ```

=== "Supervisor deployment"

    Restart the workers and confirm they are healthy:

    ```shell
    <APPNAME>ctl restart w:*
    <APPNAME>ctl status
    ```

## 6. Trigger the module path

Now trigger the event that should make the module run.

For the example above, send or replay a datapoint for the `device.hostname` attribute. For routine same-host checks, prefer `dp3 sh` and keep `curl` as a fallback.

=== "CLI (`dp3 sh`)"

    Set the config directory once for the session:

    ```shell
    export DP3_CONFIG_DIR=/path/to/config
    ```

    A manual test datapoint is often the fastest way to confirm the module is wired correctly:

    ```shell
    printf '%s\n' '[
      {
        "type": "device",
        "id": "device-123",
        "attr": "hostname",
        "v": "Example.Host",
        "src": "manual_test"
      }
    ]' | dp3 sh datapoints
    ```

    If the module emits `hostname_normalized`, verify it directly:

    ```shell
    dp3 sh entity device id device-123 attr hostname_normalized get
    ```

=== "HTTP (`curl`)"

    A manual test datapoint is often the fastest way to confirm the module is wired correctly:

    ```shell
    curl -X POST 'http://localhost:5000/datapoints' \
      -H 'Content-Type: application/json' \
      --data '[
        {
          "type": "device",
          "id": "device-123",
          "attr": "hostname",
          "v": "Example.Host",
          "src": "manual_test"
        }
      ]'
    ```

    If the module emits `hostname_normalized`, verify it through the API:

    ```shell
    curl -X GET 'http://localhost:5000/entity/device/device-123/get/hostname_normalized' \
      -H 'Accept: application/json'
    ```

## 7. Check logs and troubleshoot

For many secondary modules, worker logs are the first useful signal that the module loaded and the callback actually ran.

=== "Local shell"

    Inspect the worker terminal output directly.

=== "Docker Compose app"

    ```shell
    docker compose -f docker-compose.app.yml logs -f worker
    ```

=== "Supervisor deployment"

    ```shell
    <APPNAME>ctl status
    tail -f /var/log/<APPNAME>/worker0.log
    grep "Exception\|Error\|Traceback\|File \"" -B1 -A1 /var/log/<APPNAME>/worker*.log
    ```

If the callback path ran but the expected attribute is still missing, verify that:

- the module is listed in `enabled_modules`
- the hook is registered for the correct entity and attribute
- any emitted attribute already exists in `db_entities`
- the worker was restarted after the code or config change

If you only change the module-specific YAML later, you may be able to reload it with the [`refresh_module_config`](../configuration/control.md#refresh_module_config) control action instead of restarting workers. New code and new hook registrations still require a worker restart.

## Related pages

- [How to add an attribute](add-attribute.md)
- [How-to guides](index.md)
- [Modules](../modules.md)
- [Module Hook Reference](../hooks.md)
- [Processing core configuration](../configuration/processing_core.md)
- [Modules configuration](../configuration/modules.md)
- [Control configuration](../configuration/control.md)
