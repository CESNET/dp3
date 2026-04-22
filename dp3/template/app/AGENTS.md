# Guidelines

This is an app built on the DP3 platform; for guides, concepts, best practices
- docs index for quick reference: https://cesnet.github.io/dp3/llms.txt
- human-readable docs at: https://cesnet.github.io/dp3/

## Common commands

- Start local services: `docker compose up -d --build`
- Run the API: `APP_NAME={{DP3_APP}} CONF_DIR=config dp3 api`
- Run a worker: `dp3 worker {{DP3_APP}} config 0`
- Validate configuration: `dp3 check config`

## Project notes

- Application configuration lives in `config/`.
- Custom modules can be added under `modules/`.
- When changing configuration, validate it with `dp3 check config`.
