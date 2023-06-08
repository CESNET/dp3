from fastapi import APIRouter

from api.internal.config import CONTROL_WRITER
from api.internal.response_models import SuccessResponse
from dp3.common.control import ControlAction, ControlMessage

router = APIRouter()


@router.get("/{action}")
async def execute_action(action: ControlAction) -> SuccessResponse:
    """Sends the given action into execution queue."""
    CONTROL_WRITER.put_task(ControlMessage(action=action))
    return SuccessResponse(detail="Action sent.")
