from datetime import datetime
from typing import Literal, Optional

import requests
from fastapi import APIRouter, HTTPException
from pydantic import NonNegativeInt

from dp3.api.internal.config import TELEMETRY_READER
from dp3.common.types import AwareDatetime

router = APIRouter()


@router.get("/sources_validity")
async def get_sources_validity() -> dict[str, datetime]:
    """Get validity of all data sources.

    Returns timestamps of current validity of all sources.
    This should be latest `t2` of incoming datapoints for given data source.
    """
    return TELEMETRY_READER.get_sources_validity()


@router.get("/sources_age")
async def get_sources_age(unit: Literal["minutes", "seconds"] = "minutes") -> dict[str, int]:
    """Get current source ages in requested units."""
    return TELEMETRY_READER.get_sources_age(unit)


@router.get("/entities_per_attr")
async def get_entities_per_attr() -> dict[str, dict[str, int]]:
    """Get counts of entities with data present for each configured attribute."""
    return TELEMETRY_READER.get_entities_per_attr()


@router.get("/snapshot_summary")
async def get_snapshot_summary() -> dict:
    """Get summary of recent snapshot activity."""
    return TELEMETRY_READER.get_snapshot_summary()


@router.get("/metadata")
async def get_metadata(
    module: Optional[str] = None,
    date_from: Optional[AwareDatetime] = None,
    date_to: Optional[AwareDatetime] = None,
    skip: NonNegativeInt = 0,
    limit: NonNegativeInt = 0,
    sort: Literal["newest", "oldest"] = "newest",
) -> list[dict]:
    """Get filtered metadata documents from the internal metadata collection."""
    return TELEMETRY_READER.get_metadata(
        module=module,
        date_from=date_from,
        date_to=date_to,
        newest_first=(sort == "newest"),
        skip=skip,
        limit=limit,
    )


@router.get("/rabbitmq/queues")
async def get_rabbitmq_queues() -> dict:
    """Get RabbitMQ queue telemetry for the running application."""
    try:
        return TELEMETRY_READER.get_rabbitmq_queues()
    except requests.RequestException as e:
        raise HTTPException(status_code=503, detail=f"RabbitMQ telemetry unavailable: {e}") from e
