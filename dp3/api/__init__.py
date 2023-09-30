"""
Platfrom HTTP API implementation.

See the individual routers to see the API endpoint implementation.

- [`root`][dp3.api.routers.root] - catch-all router for the root path `/*`.
  Implements the datapoint ingestion endpoint.
- [`entity`][dp3.api.routers.entity] - implements the entity endpoints, `/{entity_type}/*`.
- [`control`][dp3.api.routers.control] - implements the control endpoints, `/{control_action}`.
"""
