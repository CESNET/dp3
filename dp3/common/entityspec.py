from typing import Any, Optional, Union

from pydantic import BaseModel, validator


class EntitySpec(BaseModel):
    """
    Entity specification

    This class represents specification of an entity type (e.g. ip, asn, ...)
    """
    id: str
    name: str
    auto_create_record: Optional[bool] = False

    def __init__(self, id: str, spec: dict[str, Union[str, bool]]):
        super().__init__(
            id=id,
            name=spec.get("name"),
            auto_create_record=spec.get("auto_create_record")
        )
