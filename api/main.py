from fastapi import FastAPI

from api.internal.models import HealthCheckResponse
from api.routers import entity

# Create new FastAPI app
app = FastAPI()

# Register routers
app.include_router(entity.router, prefix="/entity", tags=["entity"])


@app.get("/")
async def health_check() -> HealthCheckResponse:
    """Health check

    Returns simple 'It works!' response.
    """
    return HealthCheckResponse()
