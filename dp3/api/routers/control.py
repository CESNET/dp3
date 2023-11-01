from fastapi import APIRouter

from dp3.api.internal.config import CONTROL_WRITER
from dp3.api.internal.response_models import SuccessResponse
from dp3.common.control import ControlAction, ControlMessage

router = APIRouter()


@router.get("/refresh_on_entity_creation")
async def refresh_on_entity_creation(etype: str) -> SuccessResponse:
    """Sends the action `refresh_on_entity_creation` into execution queue.

    This action is only accepted with the `etype` parameter.
    """
    CONTROL_WRITER.broadcast_task(
        ControlMessage(action=ControlAction.refresh_on_entity_creation, kwargs={"etype": etype})
    )
    return SuccessResponse(detail="Action sent.")


@router.get("/{action}")
async def execute_action(action: ControlAction) -> SuccessResponse:
    """Sends the given action into execution queue."""
    CONTROL_WRITER.put_task(ControlMessage(action=action))
    return SuccessResponse(detail="Action sent.")
