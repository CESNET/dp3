# Entity Lifetimes

There are currently three supported types of lifetimes:

* **Immortal** - these entities are never removed unless explicitly ordered by the user. (default)
* **Time-to-live tokens** - The platform will keep a list of TTL tokens for each entity, and will remove the entity when all tokens expire.
* **Weak entities** - These entities are removed when they are no longer referenced by any other entity.

When the lifetime of an entity ends, the entity is removed from the database, and all its data is deleted.
How often this happens is configured in the [Garbage Collector](garbage_collector.md) configuration.

Let us examine the options in detail.

## Immortal entities

Immortal entities are never removed unless explicitly ordered by the user.
There are no special options for this lifetime type as of now, and this is the default lifetime type,
i.e. it is used when no lifetime options are specified.

An explicit configuration might look like this:

```yaml
entity:
  id: bus_line
  name: Bus Line
  snapshot: false
  lifetime:
    type: immortal  # (1)!
```

1. Buses and drivers may change, but we do not remove the serviced bus lines

## Time-to-live tokens

The platform will keep a list of TTL tokens for each entity, and will remove the entity when all of its tokens expire.
There are several options for this lifetime type:

| Option name   | type        | default    | description                                                                                                                             |
|---------------|-------------|------------|-----------------------------------------------------------------------------------------------------------------------------------------|
| `on_create`   | `timedelta` | *required* | The base lifetime of an entity that is set on creation                                                                                  |
| `mirror_data` | `bool`      | `True`     | If `True`, the lifetime of the entity is extended by the             `max_age` of the incoming observations and timeseries data-points. |

TTL tokens can be attached to new data datapoints on per-attribute basis
(see [attribute configuration](db_entities.md#base)),
set to mirror the lifetime of the data (`mirror_data`),
or sent explicitly using the API (see [`/entity/{etype}/{eid}/ttl`](../api.md#extend-ttls)).

Here are a couple of examples of how this type of lifetime can be configured:

### The Timer

The entity will receive a TTL token on its creation, and will be automatically deleted after it expires.
You can supplement this by explicitly extending the lifetime of the entity using the API, 
or explicitly configuring a TTL on an attribute that will extend the lifetime.

```yaml
entity:
  id: traffic_jam
  name: Traffic Jam
  snapshot: true
  lifetime:
    type: ttl
    on_create: 1h
    mirror_data: false
```

### Data-driven lifetime

This is the default behavior of `mirror_data` option. The entity will be kept alive for as long as there are
observations data for it. The lifetime of the entity will be extended by the `max_age` of the incoming data-points.

```yaml
entity:
  id: passenger
  name: Passenger 
  snapshot: true
  lifetime:
    type: ttl
    on_create: 30m  # (1)!
    mirror_data: true
```

1. 30 minutes.

### Mixture of both

You can combine the two previous examples to create a lifetime that is extended by the incoming observation data,
but has additional `ttl` set on incoming plain datapoints, or can be extended explicitly using the API.
The lifetime of the entity will be extended using all the sources you choose to use.

## Weak entities

The entity will be removed when it is no longer referenced by any other entity.
This is useful when you want to track some data that relates to multiple of your entities,
but want to discard it if no such entity exists anymore.
There are no special options for this lifetime type as of now.

```yaml
entity:
  id: shared_ticket
  name: Shared Ticket
  snapshot: true
  lifetime:
    type: weak
```


??? warning "Remember the other side"

    For the weak entity lifetime to work, there has to be a reference from an another entity,
    for example:
  
    ```yaml
    entity:
      id: passenger
      name: Passenger 
      snapshot: true
      lifetime:
        type: ttl
        on_create: 30m
    attribs:
      shared_tickets:
        name: Shared Tickets
        description: Tickets this passenger shares with other passengers.
        type: observations
        data_type: link<shared_ticket>
        multi_value: true
        history_params:
          pre_validity: 0
          post_validity: 0
          max_age: 14d
          aggregate: false
    ```

## Continue to

When the lifetime of an entity ends, the entity is removed from the database, and all its data is deleted.
How often this happens is configured in the [Garbage Collector](garbage_collector.md) configuration.