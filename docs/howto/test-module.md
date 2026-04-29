# Test a secondary module

DP3 includes helpers for writing focused unit tests for secondary modules without running a full
worker, database, message broker, or snapshot scheduler.

Use [`DP3ModuleTestCase`][dp3.testing.DP3ModuleTestCase] when you want to instantiate a
module with the application's real `db_entities` model and then call registered hooks directly.
The test registrar captures callbacks during module initialization and exposes runners for the
common hook families.

The config directory is read from the `DP3_CONFIG_DIR` environment variable unless a test class
sets `config_dir` explicitly. Module configuration is read from `modules.<module_name>` in that
config by default, where `<module_name>` is inferred from the module class' Python module name.

```bash
DP3_CONFIG_DIR=config python -m unittest discover -s tests -v
```

## Basic pattern

```python
from unittest.mock import patch

from dp3.testing import DP3ModuleTestCase
from modules.ip_exposure_profile import IPExposureProfile


class TestIPExposureProfile(DP3ModuleTestCase):
    module_class = IPExposureProfile

    def test_open_port_creates_service_and_link(self):
        dp = self.make_observation_datapoint("ip", "192.0.2.1", "open_ports", 443)

        tasks = self.run_on_new_attr("ip", "open_ports", "192.0.2.1", dp)

        self.assertDatapoint(tasks, etype="service", eid="192.0.2.1:443", attr="guessed_type")
        self.assertDatapoint(tasks, etype="ip", eid="192.0.2.1", attr="services")

    def test_updater_uses_mocked_external_lookup(self):
        with patch.object(self.module, "_fetch_service_intel", return_value={"risk": "high"}):
            tasks = self.run_periodic_update(
                "service",
                "192.0.2.1:443",
                {"guessed_type": "https"},
                hook_id="service_intel",
            )

        self.assertDatapoint(tasks, attr="external_risk", v="high")
```

## What the helper provides

`DP3ModuleTestCase`:

- loads `db_entities` from `DP3_CONFIG_DIR` or `config_dir` and builds a real `ModelSpec`,
- creates a minimal `PlatformConfig`,
- instantiates `module_class` with a test registrar,
- creates validated `DataPointTask` and datapoint objects using the loaded model,
- calls registered hooks directly,
- provides partial-match assertions for emitted tasks, datapoints, and mutated records.

The helper is intended for module-level unit tests. It does not run a database, task queues,
worker processes, recursive task ingestion, or full linked snapshot loading.

## Hook runners

Common runners are available on the test case:

- `run_allow_entity_creation(entity, eid, task=None)`
- `run_on_entity_creation(entity, eid, task=None)`
- `run_on_new_attr(entity, attr, eid, dp)`
- `run_correlation_hooks(entity_type, record, master_record=None)`
- `run_periodic_update(entity_type, eid, master_record, hook_id=None)`
- `run_periodic_eid_update(entity_type, eid, hook_id=None)`
- `run_scheduler_job(index_or_func)`

Correlation tests pass the snapshot `record` explicitly. The record must contain `eid`.
Scheduler jobs can be selected by registration index, callable, or callable name.

## Assertions

Assertions use partial matching: only fields supplied in the expected values are checked.

```python
self.assertDatapoint(tasks, etype="ip", attr="hostname", v="example.test")
self.assertTaskEmitted(tasks, etype="ip", eid="192.0.2.1")
self.assertNoTasks(tasks)
self.assertRecordContains(record, exposure_score=10)
```

Snake-case aliases are also available: `assert_datapoint`, `assert_task_emitted`,
`assert_no_tasks`, and `assert_record_contains`.

## Registration assertions

Use registration assertions when a test needs to verify callback coverage or dynamic hook
registration.

```python
self.assert_registered("on_new_attr", entity="ip", attr="hostname")
self.assert_registered_once("correlation", entity_type="service")
self.assert_registered_attrs("service", expected_service_attrs)
self.assert_scheduler_registered(func="reload_ip_groups", minute="*/10")
```

`assert_scheduler_registered()` accepts scheduler fields such as `minute`, `hour`, and `second`,
along with `func` for matching the registered callable by object or function name.

## Mocking external dependencies

Patch external constructors or functions before module instantiation when the dependency is created
in `__init__` or `load_config`:

```python
class TestDNSModule(DP3ModuleTestCase):
    module_class = DNSModule

    def setUp(self):
        self.resolver_patcher = patch("modules.dns_module.Resolver", FakeResolver)
        self.resolver_patcher.start()
        self.addCleanup(self.resolver_patcher.stop)
        super().setUp()
```

If patching is not convenient, use a test subclass as `module_class` and override the module's
initialization or dependency construction while keeping the hook methods under test unchanged.

Deprecated registrar methods (`register_entity_hook` and `register_attr_hook`) are supported by the
test registrar and emit `DeprecationWarning`. Prefer the modern registration methods in new module
code and tests.
