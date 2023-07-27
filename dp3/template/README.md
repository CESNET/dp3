# {{DP3_APP}}

An application built using the DP3 platform.

Run the DP3 platform dependencies using docker compose:

```shell
docker compose up -d --build
```

Run the application using docker compose:

```shell
docker-compose -f docker-compose.app.yml up -d --build
```

Or run the `worker` process and the `api` directly in your terminal for easier debugging.

To run `worker`:

```shell
dp3 worker {{DP3_APP}} config 0     
```

To run api:
```shell
APP_NAME={{DP3_APP}} CONF_DIR=config dp3 api
```

You can validate your configuration after changes using the `check` command,
which will pinpoint any errors in your configuration (especially of DB entities):

```shell
dp3 check config
```

**DP3 documentation**: https://cesnet.github.io/dp3/

**Swagger API documentation** (once the app is running): http://localhost:5000/docs/
