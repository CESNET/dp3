# Control

The `Control` module serves the [`/control/...`](../api.md#control) endpoint.
This is used to execute actions that are not part of the normal operation of the application,
at a time when the user decides to do so.
Useful for management of the application, especially when there are changes made to the configuration.

The `control.yml` file looks like this: 

```yaml
allowed_actions: [
  make_snapshots, # Makes an out-of-order snapshot of all entities
]
```

The `allowed_actions` parameter is a list of actions that are allowed to be executed.
The way to use this file is to simply comment out the actions that you do not want to be allowed.