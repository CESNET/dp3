# Control

The `Control` module serves the [`/control/...`](../api.md#control) endpoints.
This is used to execute actions that are not part of the normal operation of the application,
at a time when the user decides to do so.
Useful for management of the application, especially when there are changes made to the configuration.

The `control.yml` file looks like this: 

```yaml
allowed_actions: [
  make_snapshots, # (1)!
  refresh_on_entity_creation, # (2)!
  refresh_module_config, # (3)!
]
```

1. Makes an out-of-order snapshot of all entities
2. Re-runs the `on_entity_creation` callback for selected `etype`
3. Re-runs the `load_config` for selected module and will refresh the values derived by the module

The `allowed_actions` parameter is a list of actions that are allowed to be executed.
The way to use this file is to simply comment out the actions that you do not want to be allowed.

## Actions

The swagger API documentation provides a simple GUI for executing the actions,
but they can also be executed directly by sending a `GET` request to the endpoint.
These are the actions that are currently available:

### `make_snapshots`

This action will make an out-of-order snapshot of all entities.

### `refresh_on_entity_creation`

This action will re-run the `on_entity_creation` callback for a selected `etype`, which is passed as a query parameter.
Will fail if the `etype` is not provided.

### `refresh_module_config`

This action will re-run the `load_config` for selected module and will refresh the values derived by the module.
The module name is to be passed as a query parameter and the action will fail without it.

The refreshing part of this action covers the `on_entity_creation` and `on_new_attr` callbacks
when the `refresh` argument is set on callback registration, otherwise no refresh is performed,
only the `load_config` is re-run.

