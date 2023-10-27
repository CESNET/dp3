from datetime import datetime

from fastapi import APIRouter

from dp3.api.internal.config import TELEMETRY_READER

router = APIRouter()


@router.get("/sources_validity")
async def get_sources_validity() -> dict[str, datetime]:
    """Get validity of all data sources

    Returns timestamps (datetimes) of current validity of all sources.
    This should be latest `t2` of incoming datapoints for given data source.
    """
    return TELEMETRY_READER.get_sources_validity()
