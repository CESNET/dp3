# How to add an attribute

This guide walks through adding a new attribute to an existing DP³ application, rolling the change out, and verifying that the attribute is accepted end to end.

The procedure is the same whether the datapoints come from an external producer, a primary input module, or a secondary DP³ module. DP³ only cares that the attribute is defined in the model before datapoints for it arrive.

This guide assumes you already know the intended data model for the attribute: its entity type, attribute type, and data type. If you still need to decide that, see the [Data model](../data_model.md), [History Management](../history_management.md), and [DB entities configuration](../configuration/db_entities.md) pages first.

If you do not already have a local DP³ application running for development, start with [Get started with local DP³ app development](get-started.md).

## 1. Add the attribute to `db_entities`

Edit the entity specification file in `config/db_entities/<entity_type>.yml` and add the new attribute under `attribs`.

For example, to add an observation attribute called `risk_score` to the `device` entity type:

```yaml title="config/db_entities/device.yml"
attribs:
  risk_score:
    name: Risk score
    description: Risk score reported by an enrichment pipeline.
    type: observations
    data_type: float
    history_params:
      max_age: 14d
      pre_validity: 0s
      post_validity: 1h
```

If the entity type already exists, adding a new attribute is an additive schema change. DP³ can apply additive schema changes automatically when workers start. The manual `dp3 schema-update` workflow is only needed for destructive or incompatible schema changes such as deleting attributes or changing their type.

## 2. Validate the configuration

Before restarting anything, validate the whole configuration:

```shell
dp3 check /path/to/config
```

Fix any reported errors before continuing.

## 3. Reload the API and worker processes

The running API and workers must reload the configuration before they will recognize the new attribute.

=== "Local shell"

    Restart the API and every worker process that uses the configuration:

    ```shell
    APP_NAME=my_app CONF_DIR=config dp3 api
    dp3 worker my_app config 0
    ```

    If you run multiple worker processes, restart each one.

=== "Docker Compose app"

    Recreate the application containers so they pick up the updated configuration:

    ```shell
    docker compose -f docker-compose.app.yml up -d --build
    docker compose -f docker-compose.app.yml ps
    ```

=== "Supervisor deployment"

    Restart the API and workers managed by supervisor, then check that all processes are healthy:

    ```shell
    <APPNAME>ctl restart api w:*
    <APPNAME>ctl status
    ```

If your deployment also has other processes that cache or expose the model, reload them as well.

## 4. Start sending datapoints for the new attribute

Once the configuration is live, enable the producer that emits the new attribute:

- For an external producer or primary input module, start sending datapoints to the DP³ API.
- For a secondary DP³ module, restart the worker if needed, then trigger the callback path that emits the datapoint.

A quick way to prove the configuration works independently of your real producer is to submit one test datapoint manually. Using the `risk_score` example above:

```shell
curl -X POST 'http://localhost:5000/datapoints' \
  -H 'Content-Type: application/json' \
  --data '[
    {
      "type": "device",
      "id": "device-123",
      "attr": "risk_score",
      "v": 0.82,
      "t1": "2026-04-21T12:00:00Z",
      "t2": "2026-04-21T12:05:00Z",
      "src": "manual_test"
    }
  ]'
```

Adjust the URL, entity type, entity id, attribute id, and payload to match your application.

## 5. Verify through the API

Prefer API-level verification first.

Start by reading the attribute directly:

```shell
curl -X GET 'http://localhost:5000/entity/device/device-123/get/risk_score' \
  -H 'Accept: application/json'
```

What you should expect depends on the attribute type:

- `plain` attributes return the current value.
- `observations` attributes return the current value together with history.
- `timeseries` attributes return history samples.

Then inspect the full master record if you want to see the attribute in context:

```shell
curl -X GET 'http://localhost:5000/entity/device/device-123/master' \
  -H 'Accept: application/json'
```

If snapshots are enabled for the entity type, you can also check the snapshot view after a snapshot run:

```shell
curl -X GET 'http://localhost:5000/entity/device/device-123/snapshots' \
  -H 'Accept: application/json'
```

## 6. Deeper checks and troubleshooting

If the attribute still does not appear where you expect it, use the checks below.

### Check logs

=== "Local shell"

    Inspect the terminal output of the API and worker processes you restarted.

=== "Docker Compose app"

    Follow the API and worker logs:

    ```shell
    docker compose -f docker-compose.app.yml logs -f api worker
    ```

=== "Supervisor deployment"

    Check the process status and inspect the worker logs:

    ```shell
    <APPNAME>ctl status
    tail -f /var/log/<APPNAME>/worker0.log
    grep "Exception\|Error\|Traceback\|File \"" -B1 -A1 /var/log/<APPNAME>/worker*.log
    ```

For secondary DP³ modules, worker logs are often the first place where callback failures or type mismatches become visible.

### Check MongoDB directly

Use direct database inspection when API-level checks are not enough, especially when debugging raw ingestion or module-produced datapoints.

```shell
mongosh "mongodb://<user>:<password>@<host>:<port>/"
```

```javascript
use <db_name>
entity = "device";
attr = "risk_score";

// `#raw` contains incoming datapoints.
db.getCollection(`${entity}#raw`).findOne({attr: attr})
db.getCollection(`${entity}#raw`).find({attr: attr}).sort({t1: -1}).limit(5)

// `#master` contains the current stored state.
// Plain attribute
// db.getCollection(`${entity}#master`).find({[attr]: {$exists: true}}).limit(5)

// Observations attribute
// db.getCollection(`${entity}#master`).find({$and: [{[attr]: {$exists: true}}, {[attr]: {$ne: []}}]}).limit(5)
```

To print only the stored value of a plain attribute:

```javascript
(db
  .getCollection(`${entity}#master`)
  .find({[attr]: {$exists: true}})
  .limit(5)
  .forEach((x) => print(x[attr]["v"]))
)
```

To print compact observation history:

```javascript
(db
  .getCollection(`${entity}#master`)
  .find({$and: [{[attr]: {$exists: true}}, {[attr]: {$ne: []}}]})
  .limit(5)
  .forEach((y) => {
    print(y["_id"]);
    y[attr].forEach((z) => print(z["t1"], z["t2"], z["v"]));
    print();
  })
)
```

## Common failure modes

- The API or workers are still running with the old configuration → [Restart](#3-reload-the-api-and-worker-processes)
- The datapoint shape does not match the configured type, for example a wrong `data_type` or missing `t1`/`t2` for non-plain attributes. → Check the error returned from API.
- A secondary module callback was never registered → Register the [module hook](../hooks.md)

## Related pages

- [How-to guides](index.md)
- [Configuration overview](../configuration/index.md)
- [DB entities configuration](../configuration/db_entities.md)
- [History manager configuration](../configuration/history_manager.md)
- [Entity lifetimes](../configuration/lifetimes.md)
- [Modules](../modules.md)
- [API](../api.md)
