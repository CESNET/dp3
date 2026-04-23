# How to add an input module

This guide walks through integrating a new input producer with an existing DP³ application.

Here, an *input module* means anything outside the DP³ worker process that collects data and sends datapoints to the DP³ HTTP API: a standalone script, a service, a scheduled job, or an adapter around another system.

This guide focuses on the DP³ side of the integration. It assumes you already know how the input module obtains its source data.

If you do not already have a local DP³ application running for development, start with [Get started with local DP³ app development](get-started.md).

## 1. Make sure the target attributes exist

Before the input module sends anything, DP³ must know the target entity and attribute definitions.

If the input will send a new attribute, follow [How to add an attribute](add-attribute.md) first. That guide covers:

- adding the attribute to `config/db_entities`
- validating the configuration
- rolling the model change out to the API and workers
- verifying that the new attribute is accepted

If the input only sends datapoints for attributes that already exist, you can continue directly.

## 2. Confirm that the API is reachable

A new input module should be tested against a running DP³ API before you debug the producer itself. For routine same-host checks, prefer `dp3 sh` and keep `curl` as a fallback.

=== "CLI (`dp3 sh`)"

    Set the config directory once for the session:

    ```shell
    export DP3_CONFIG_DIR=/path/to/config
    ```

    Then run the health check:

    ```shell
    dp3 sh health
    ```

=== "HTTP (`curl`)"

    ```shell
    curl -X GET 'http://localhost:5000/' \
      -H 'Accept: application/json'
    ```

A healthy API responds with:

```json
{
  "detail": "It works!"
}
```

If your application runs elsewhere, replace the base URL with the correct host, port, or reverse-proxy path.

## 3. Prepare the datapoint payload

The input module should send datapoints that match the configured model exactly:

- `type` must match the entity type id
- `id` must match the configured entity id type
- `attr` must match the attribute id
- `v` must match the configured data type
- `t1` and `t2` must be present for observations and timeseries datapoints

A simple observations datapoint looks like this:

```json
{
  "type": "device",
  "id": "device-123",
  "attr": "risk_score",
  "v": 0.82,
  "t1": "2026-04-21T12:00:00Z",
  "t2": "2026-04-21T12:05:00Z",
  "src": "my_input_module"
}
```

For the full datapoint format, see the [API reference](../api.md#insert-datapoints).

## 4. Send a manual test datapoint first

Before running the real input module, send one datapoint manually. This narrows the problem down to either:

- DP³ configuration and API handling, or
- the input module implementation itself

=== "CLI (`dp3 sh`)"

    ```shell
    printf '%s\n' '[
      {
        "type": "device",
        "id": "device-123",
        "attr": "risk_score",
        "v": 0.82,
        "t1": "2026-04-21T12:00:00Z",
        "t2": "2026-04-21T12:05:00Z",
        "src": "manual_test"
      }
    ]' | dp3 sh datapoints
    ```

=== "HTTP (`curl`)"

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

Once the manual request works, point your input module at the same API endpoint and send the same shape of payload.

## 5. Run the input module

Now start the real producer and let it send datapoints to the API.

What this means depends on the input module itself:

- start the long-running service
- run the script manually
- enable the scheduled job
- replay a sample dataset into the producer

It is worth keeping one datapoint field stable during testing, especially `src`, so you can recognize the producer in logs and stored records.

## 6. Verify that data is flowing through DP³

Start with API-level verification. For routine checks on the same host, prefer `dp3 sh` and keep `curl` as a fallback.

=== "CLI (`dp3 sh`)"

    To read one attribute directly:

    ```shell
    dp3 sh entity device id device-123 attr risk_score get
    ```

    To inspect the full master record for the entity:

    ```shell
    dp3 sh entity device id device-123 master
    ```

=== "HTTP (`curl`)"

    To read one attribute directly:

    ```shell
    curl -X GET 'http://localhost:5000/entity/device/device-123/get/risk_score' \
      -H 'Accept: application/json'
    ```

    To inspect the full master record for the entity:

    ```shell
    curl -X GET 'http://localhost:5000/entity/device/device-123/master' \
      -H 'Accept: application/json'
    ```

If the attribute shows up for the manually sent datapoint but not for the real input module, the remaining problem is in the producer, not in the DP³ model.

## 7. Deeper checks and troubleshooting

### Check the API response first

If `POST /datapoints` returns an error, fix that before inspecting workers or the database. Validation errors are often enough to tell you whether:

- the attribute id is wrong
- the value type does not match
- timestamps are missing

### Check API and worker logs

If the request succeeds but the data still does not appear as expected, inspect the running DP³ processes.

=== "Local shell"

    Inspect the API and worker terminals directly.

=== "Docker Compose app"

    ```shell
    docker compose -f docker-compose.app.yml logs -f api worker
    ```

=== "Supervisor deployment"

    ```shell
    <APPNAME>ctl status
    tail -f /var/log/<APPNAME>/api.log
    tail -f /var/log/<APPNAME>/worker0.log
    ```

### Inspect raw ingestion when needed

If you need to distinguish between ingestion and later processing, inspect current raw datapoints first.
Prefer the CLI path for common troubleshooting, keep the query narrow because raw inspection can be
slow on large collections, and keep the `mongosh` flow as a fallback.

=== "CLI (`dp3 sh`)"

    ```shell
    export DP3_CONFIG_DIR=/path/to/config
    dp3 sh entity device raw \
      --attr risk_score \
      --limit 5 \
      --format ndjson
    ```

    If you need candidate entity ids for follow-up checks, list entities whose latest snapshot has
    data for the attribute and extract their ids:

    ```shell
    dp3 sh entity device list \
      --has-attr risk_score \
      --limit 5 \
      | jq -r '.eid'
    ```

=== "MongoDB (`mongosh`)"

    ```javascript
    use <db_name>
    entity = "device";
    attr = "risk_score";

    db.getCollection(`${entity}#raw`).find({attr: attr}).sort({t1: -1}).limit(5)
    ```

If the datapoint is present in `#raw` but not visible where you expect it later, inspect the model,
worker logs, and attribute definition again.

## Common failure modes

- The producer sends to the wrong API URL.
- The payload shape does not match the configured attribute type.
- The attribute was not added to `db_entities` before the producer started sending it.
- The API accepted the request, but workers are not running or are using old configuration.
- The producer is sending the wrong entity id or entity type, so you are looking in the wrong place during verification.

## Related pages

- [How to add an attribute](add-attribute.md)
- [How-to guides](index.md)
- [API](../api.md)
- [Configuration overview](../configuration/index.md)
- [DB entities configuration](../configuration/db_entities.md)
