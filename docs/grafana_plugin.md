# Grafana plugin

To simplify access and visualization of DP³ instance data,
we've created DP³ **[Grafana](https://grafana.com/grafana/) plugin**.

The plugin includes DP³ **data source** support,
panel for displaying history of multi value attributes,
as well as a **dashboard generator**.

This approach allows you to combine multiple data sources (including non-DP³)
and visualize them together in **single unified interface**.

## Installation overview

DP³ Grafana plugin has been reviewed by Grafana Labs and
is available signed from standard
**[plugin catalog](https://grafana.com/grafana/plugins/cesnet-dp3-app/)**.

You can install it from GUI or use CLI interface:

```sh
grafana-cli plugins install cesnet-dp3-app
```

## Details

For more details, please follow README in Grafana plugin catalog:
**[https://grafana.com/grafana/plugins/cesnet-dp3-app/](https://grafana.com/grafana/plugins/cesnet-dp3-app/)**

Source code is available at: [https://github.com/CESNET/dp3-grafana/](https://github.com/CESNET/dp3-grafana/)
