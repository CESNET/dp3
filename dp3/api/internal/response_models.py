from fastapi import HTTPException
from pydantic import BaseModel


class HealthCheckResponse(BaseModel):
    """Healthcheck endpoint response"""

    detail: str = "It works!"


class SuccessResponse(BaseModel):
    """Generic success response"""

    detail: str = "OK"


class ErrorResponse(BaseModel):
    """Generic error response"""

    detail: str


class RequestValidationError(HTTPException):
    """HTTP exception wrapper to simplify path and query validation"""

    def __init__(self, loc, msg):
        super().__init__(422, [{"loc": loc, "msg": msg, "type": "value_error"}])
