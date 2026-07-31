# `dp3 sh` command reference

`dp3 sh` is a shell-oriented client for reading from and writing to a running DP³ API. It formats API responses as JSON or newline-delimited JSON and provides commands for datapoints, entities, control actions, telemetry, and shell completion.

On a deployment host, you can use the generated `<APPNAME>sh` wrapper in place of `dp3 sh`. The wrapper supplies the application's configuration directory. Otherwise, select a configuration with `--config` or `DP3_CONFIG_DIR`, and use `--url` when the API is not available through the automatically probed localhost URLs.

{{ dp3_sh_help() }}
