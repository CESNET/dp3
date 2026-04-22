# API

DP³'s has HTTP API which you can use to post datapoints and to read data stored in DP³.
As the API is made using FastAPI, there is also an interactive documentation available at `/docs` endpoint.

If you are wiring a new producer into a DP³ application, start with [How to add an input module](howto/add-input.md), then use this page as the endpoint reference.

There are several API endpoints:

- [`GET /`](#index): check if API is running (just returns `It works!` message)
- [`POST /datapoints`](#insert-datapoints): insert datapoints into DP³
- [`GET /entity/<entity_type>/get`](#get-entities): get current snapshots of entities of entity type
- [`GET /entity/<entity_type>/count`](#count-entities): get total document count for query of entity type
- [`GET /entity/<entity_type>/<entity_id>`](#get-eid-data): get data of the entity identified by type and id
- [`GET /entity/<entity_type>/<entity_id>/master`](#get-master-record): get the master record of the entity identified by type and id
- [`GET /entity/<entity_type>/<entity_id>/snapshots`](#get-snapshots): get snapshot history of the entity identified by type and id, optionally with `skip` and `limit`
- [`GET /entity/<entity_type>/<entity_id>/get/<attr_id>`](#get-attr-value): get attribute value
- [`POST /entity/<entity_type>/<entity_id>/set/<attr_id>`](#set-attr-value): set attribute value
- [`GET /entity/<entity_type>/_/distinct/<attr_id>`](#get-distinct-values): get distinct attribute values and their counts based on latest snapshots
- [`DELETE /entity/<entity_type>/<entity_id>`](#delete-eid-data): delete entity data for given id
- [`POST /entity/<entity_type>/<entity_id>/ttl`](#extend-ttls): extend TTLs of the specified entity
- [`GET /entities`](#entities): list entity configuration
- [`GET /control/<action>`](#control): send a pre-defined action into execution queue.
- [`GET /telemetry/sources_validity`](#source-validity): get timestamps of latest data from each source
- [`GET /telemetry/sources_age`](#source-age): get source ages in seconds or minutes
- [`GET /telemetry/entities_per_attr`](#entities-per-attribute): get entity counts for each configured attribute
- [`GET /telemetry/snapshot_summary`](#snapshot-summary): get summary of recent snapshot activity
- [`GET /telemetry/metadata`](#metadata): browse metadata records stored in `#metadata`
- [`GET /telemetry/rabbitmq/queues`](#rabbitmq-queues): get RabbitMQ queue telemetry for the running application

---

## Index

Health check.

### Request

`GET /`

### Response

**`200 OK`**:

`{
  "detail": "It works!"
}`

---


## Insert datapoints

### Request

`POST /datapoints`

All data are written to DP³ in the form of **datapoints**. A datapoint sets a value of a given attribute of given entity.

It is a JSON-encoded object with the set of keys defined in the table below. Presence of some keys depends on the primary type of the attribute (plain/observations/timseries).

Payload to this endpoint is JSON array of datapoints. For example:

```json
[
   { DATAPOINT1 },
   { DATAPOINT2 }
]
```

| Key    | Description                                                      | Data-type                | Required?              | Plain | Observations | Timeseries |
|--------|------------------------------------------------------------------|--------------------------|------------------------|-------|--------------|------------|
| `type` | Entity type                                                      | string                   | mandatory              | ✔     | ✔            | ✔          |
| `id`   | Entity identification                                            | string                   | mandatory              | ✔     | ✔            | ✔          |
| `attr` | Attribute name                                                   | string                   | mandatory              | ✔     | ✔            | ✔          |
| `v`    | The value to set, depends on attr. type and data-type, see below | --                       | mandatory              | ✔     | ✔            | ✔          |
| `t1`   | Start time of the observation interval                           | string (RFC 3339 format) | mandatory              | --    | ✔            | ✔          |
| `t2`   | End time of the observation interval                             | string (RFC 3339 format) | optional, default=`t1` | --    | ✔            | ✔          |
| `c`    | Confidence                                                       | float (0.0-1.0)          | optional, default=1.0  | --    | ✔            | ✔          |
| `src`  | Identification of the information source                         | string                   | optional, default=""   | ✔     | ✔            | ✔          |

More details depends on the particular type of the attribute.

#### Examples of datapoints

##### Plain

```json
{
  "type": "ip",
  "id": "192.168.0.1",
  "attr": "note",
  "v": "My home router",
  "src": "web_gui"
}
```

##### Observations

```json
{
  "type": "ip",
  "id": "192.168.0.1",
  "attr": "open_ports",
  "v": [22, 80, 443],
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:10:00",
  "src": "open_ports_module"
}
```

##### Timeseries

**`regular`**:

```json
{
  ...
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:20:00", // assuming time_step = 5 min
  "v": {
    "a": [1, 3, 0, 2]
  }
}
```

**`irregular`**: timestamps must always be present

```json
{
  ...
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:05:00",
  "v": {
    "time": ["2022-08-01T12:00:00", "2022-08-01T12:01:10", "2022-08-01T12:01:15", "2022-08-01T12:03:30"],
    "x": [0.5, 0.8, 1.2, 0.7],
    "y": [-1, 3, 0, 0]
  }
}
```

**`irregular_interval`**:

```json
{
  ...
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:05:00",
  "v": {
    "time_first": ["2022-08-01T12:00:00", "2022-08-01T12:01:10", "2022-08-01T12:01:15", "2022-08-01T12:03:30"],
    "time_last": ["2022-08-01T12:01:00", "2022-08-01T12:01:15", "2022-08-01T12:03:00", "2022-08-01T12:03:40"],
    "x": [0.5, 0.8, 1.2, 0.7],
    "y": [-1, 3, 0, 0]
  }
}
```

##### Relations

Can be represented using both **plain** attributes and **observations**. The difference will be only
in time specification. Two examples using observations:

**no data - `link<mac>`**: Sent as a dictionary with a single `"eid"` key.

```json
{
  "type": "ip",
  "id": "192.168.0.1",
  "attr": "mac_addrs",
  "v": {"eid": "AA:AA:AA:AA:AA"},
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:10:00"
}
```

**with additional data - `link<ip, int>`**: Sent as a dictionary with `"eid"` and `"data"` keys.

```json
{
  "type": "ip",
  "id": "192.168.0.1",
  "attr": "ip_dep",
  "v": {"eid": "192.168.0.2", "data": 22},
  "t1": "2022-08-01T12:00:00",
  "t2": "2022-08-01T12:10:00"
}
```

### Response

**`200 OK`**:

```
Success
```

**`400 Bad request`**:

Returns some validation error message, for example:

```
1 validation error for DataPointObservations_some_field
v -> some_embedded_dict_field
  field required (type=value_error.missing)
```

---

## Get entities

Get a list of latest snapshots of all ids present in database under entity type,
filtered by `generic_filter` and `fulltext_filters`.
Contains only the latest snapshot per entity.

Uses pagination, default limit is 20, setting to 0 will return all results.

Fulltext filters are interpreted as regular expressions.
Only string values may be filtered this way. There's no validation that queried attribute
can be fulltext filtered.
Only plain and observation attributes with string-based data types can be queried.
Array and set data types are supported as well as long as they are not multi value
at the same time.
If you need to filter EIDs, use attribute `eid`.

Generic filter allows filtering using generic MongoDB query (including `$and`, `$or`,`$lt`, etc.).
For querying non-JSON-native types, you can use the following magic strings,
as are defined by the search & replace [`magic`][dp3.database.magic] module.

There are no attribute name checks (may be added in the future).

Generic and fulltext filters are merged - fulltext overrides conflicting keys.

### Request

`GET /entity/<entity_type>/get`

**Optional query parameters:**

- skip: how many entities to skip (default: 0)
- limit: how many entities to return (default: 20)
- fulltext_filters: dictionary of fulltext filters (default: no filters)
- generic_filter: dictionary of generic filters (default: no filters)

### Response

```json
{
  "time_created": "2023-07-04T12:10:38.827Z",
  "count": 1,
  "data": [
    {}
  ]
}
```

---

## Count entities

Count latest snapshots of all ids present in database under entity type,
filtered by `generic_filter` and `fulltext_filters`.
See [`GET /entity/<entity_type>/get`](#get-entities) for details on filter format.

### Request

`GET /entity/<entity_type>/count`

**Optional query parameters:**

- fulltext_filters: dictionary of fulltext filters (default: no filters)
- generic_filter: dictionary of generic filters (default: no filters)

### Response

```json
{
  "total_count": 0
}
```

---

## Get Eid data

Get data of the entity identified by `entity_type` and `entity_id`.

Contains all snapshots and master record. Snapshots are ordered by ascending creation time.

### Request

`GET /entity/<entity_type>/<entity_id>`

**Optional query parameters:**

- date_from: date-time string
- date_to: date-time string

### Response

```json
{
  "empty": true,
  "master_record": {},
  "snapshots": [
    {}
  ]
}
```

---

## Get master record

Get the master record of the entity identified by `entity_type` and `entity_id`.

### Request

`GET /entity/<entity_type>/<entity_id>/master`

**Optional query parameters:**

- date_from: date-time string
- date_to: date-time string

### Response

```json
{
  "attr1": "value",
  "attr2": []
}
```

---

## Get snapshots

Get snapshot history of the entity identified by `entity_type` and `entity_id`.

This endpoint returns matching snapshots ordered by ascending creation time.
By default, all matching snapshots are returned. Optional `skip` and `limit` parameters can be used
for paged access without changing the response shape.

### Request

`GET /entity/<entity_type>/<entity_id>/snapshots`

**Optional query parameters:**

- date_from: date-time string
- date_to: date-time string
- skip: how many snapshots to skip (default: 0)
- limit: how many snapshots to return (`0` means no limit)

### Response

```json
[
  {}
]
```

---

## Get attr value

Get attribute value

Value is either of:

- current value: in case of plain attribute
- current value and history: in case of observation attribute
- history: in case of timeseries attribute

### Request

`GET /entity/<entity_type>/<entity_id>/get/<attr_id>`

**Optional query parameters:**

- date_from: date-time string
- date_to: date-time string

### Response

```json
{
  "attr_type": 1,
  "current_value": "string",
  "history": []
}
```

---

## Set attr value

Set current value of attribute

Internally just creates datapoint for specified attribute and value.

This endpoint is meant for `editable` plain attributes -- for direct user edit on DP3 web UI.

### Request

`POST /entity/<entity_type>/<entity_id>/set/<attr_id>`

**Required request body:**

```json
{
  "value": "string"
}
```

### Response

```json
{
  "detail": "OK"
}
```

---

## Get distinct values

Gets distinct attribute values and their counts based on latest snapshots

Useful for displaying `<select>` enumeration fields.

Works for all plain and observation data types except `dict` and `json`.

### Request

`GET /entity/<entity_type>/_/distinct/<attr_id>`

### Response

```json
{
  "value1": 10,
  "value2": 5,
  "value3": 43
}
```

---

## Delete Eid data

Delete master record and snapshots with the specified `etype` and `eid`.

Raw datapoints are not deleted,
and the entity can be restored by sending new datapoints with the same `etype` and `eid`.

### Request

`DELETE /entity/<entity_type>/<entity_id>`

### Response

```json
{
  "detail": "OK"
}
```

---

## Extend TTLs

Extend TTLs of the specified entity.

Raw datapoints are not deleted,
and the entity can be restored by sending new datapoints with the same `etype` and `eid`.

### Request

`POST /entity/<entity_type>/<entity_id>/ttl`

The request body must be a dictionary of TTLs to extend, with string keys to identify the type of TTL.
The values must be **UTC timestamps**, for example:

```json
{
  "user_interaction": "2024-10-01T12:03:00",
  "api_dependency": "2024-10-08T12:00:00"
}
```

TTLs of the same name will be extended, and you add as many TTL names as you want.

### Response

```json
{
  "detail": "OK"
}
```

---
## Entities

List entity types

Returns dictionary containing all entity types configured -- their simplified configuration and current state information.

### Request

`GET /entities`

### Response

```json
{
  "<entity_id>": {
    "id": "<entity_id>",
    "id_data_type": "<entity_spec.id_data_type>",
    "name": "<entity_spec.name>",
    "attribs": "<MODEL_SPEC.attribs(e_id)>",
    "eid_estimate_count": "<DB.estimate_count_eids(e_id)>"
  },
  ...
}
```

---

## Control

Execute Action - Sends the given action into execution queue.

You can see the enabled actions in [`/config/control.yml`](configuration/control.md), available are:

- `make_snapshots` - Makes an out-of-order snapshot of all entities
- `refresh_on_entity_creation` - Re-runs the `on_entity_creation` callback for selected `etype`
- `refresh_module_config` - Re-runs the `load_config` for selected module and will refresh the values derived by the module when configured to do so

You can learn more about the actions in the [Actions](configuration/control.md#actions) section of the `Control` configuration documentation.

### Request

`GET /control/<action>`

### Response

```json
{
  "detail": "OK"
}
```

---

## Source validity

Returns information about the validity of the data sources, i.e. when the last datapoint was received from each source.

### Request

`GET /telemetry/sources_validity`

### Response

```json
{
  "module1@collector1": "2023-10-03T11:59:58.063000",
  "module2@collector1": "2023-12-06T09:09:37.165000",
  "module3@collector2": "2023-12-08T15:52:55.282000"
}
```

---

## Source age

Returns age of the latest datapoint from each source.

### Request

`GET /telemetry/sources_age`

**Optional query parameters:**

- unit: `minutes` or `seconds` (default: `minutes`)

### Response

```json
{
  "module1@collector1": 15,
  "module2@collector1": 42
}
```

---

## Entities per attribute

Returns counts of entities for which each configured attribute currently has data present.

### Request

`GET /telemetry/entities_per_attr`

### Response

```json
{
  "ip": {
    "open_ports": 1245,
    "tags": 1198
  },
  "device": {
    "risk_score": 421
  }
}
```

---

## Snapshot summary

Returns a summary of recent snapshot activity based on metadata stored by `SnapShooter`.

### Request

`GET /telemetry/snapshot_summary`

### Response

```json
{
  "latest_age": 73.2,
  "finished_age": 1873.5,
  "entities": 123456,
  "total_s": 18.4
}
```

Fields may be `null` when the corresponding snapshot run has not been observed yet.

---

## Metadata

Browse records from the internal `#metadata` collection.

This endpoint is useful for operational inspection of metadata written by modules such as
`SnapShooter`, `HistoryManager`, and other components using `db.save_metadata` / `db.update_metadata`.

### Request

`GET /telemetry/metadata`

**Optional query parameters:**

- module: filter by metadata module name
- date_from: date-time string
- date_to: date-time string
- skip: how many metadata records to skip (default: 0)
- limit: how many metadata records to return (`0` means no limit)
- sort: `newest` or `oldest` (default: `newest`)

### Response

```json
[
  {
    "#module": "SnapShooter",
    "#worker": 0,
    "#time_created": "2026-04-22T08:11:10.000Z"
  }
]
```

---

## RabbitMQ queues

Returns RabbitMQ queue telemetry for the running application.

The API derives RabbitMQ connection details from the configured message broker settings.
Only queues belonging to the current DP³ application are returned.

### Request

`GET /telemetry/rabbitmq/queues`

### Response

```json
{
  "queues": [
    {
      "name": "my_app-worker-0",
      "queue": "0-main",
      "total": 0,
      "ready": 0,
      "unacked": 0,
      "consumers": 1,
      "memory": 12345,
      "message_bytes": 0,
      "incoming": 0.0,
      "outgoing": 0.0
    }
  ]
}
```
