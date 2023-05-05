from fastapi import APIRouter
from pydantic import NonNegativeInt, PositiveInt

from api.internal.config import DB

router = APIRouter()


@router.get("/{entity}")
async def list_entity_eids(
    entity: str, skip: NonNegativeInt = 0, limit: PositiveInt = 20
) -> list[dict]:
    """List `id`s present in database under `entity`

    Contains only master record + latest snapshot.

    Uses pagination.
    """
    # TODO: entity validation
    cursor = DB.get_master_records(entity).skip(skip).limit(limit)
    return list(cursor)
