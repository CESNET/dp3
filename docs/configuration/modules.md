# Modules

Folder `modules/` optionally contains any module-specific configuration.

This configuration doesn't have to follow any required format (except being YAML files).

In secondary modules, you can access the configuration:

```py
from dp3 import g

print(g.config["modules"]["MODULE_NAME"])
```

Here, the `MODULE_NAME` corresponds to `MODULE_NAME.yml` file in `modules/` folder.
