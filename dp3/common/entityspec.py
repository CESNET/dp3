from pydantic import BaseModel


class EntitySpec(BaseModel):
    """Entity specification

    This class represents specification of an entity type (e.g. ip, asn, ...)
    """

    id: str
    name: str
    snapshot: bool

    description: str = ""
