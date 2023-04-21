from pydantic import BaseModel


class HealthCheckResponse(BaseModel):
    """Healthcheck endpoint response"""

    detail: str = "It works!"
