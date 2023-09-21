from pydantic import BaseModel, Extra


class SpecModel(BaseModel, extra=Extra.forbid):
    ...


class EntitySpec(SpecModel):
    """Entity specification

    This class represents specification of an entity type (e.g. ip, asn, ...)
    """

    id: str
    name: str
    snapshot: bool

    description: str = ""
