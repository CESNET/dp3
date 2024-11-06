from datetime import datetime
from typing import Annotated, Any, Optional, Union

from pydantic import BaseModel, Field, NonNegativeInt, PlainSerializer, model_validator

from dp3.common.attrspec import AttrSpec, AttrSpecGeneric, AttrSpecType, AttrType
from dp3.common.datapoint import to_json_friendly
from dp3.common.entityspec import EntitySpec, entity_context


class EntityState(BaseModel):
    """Entity specification and current state

    Merges (some) data from DP3's `EntitySpec` and state information from `Database`.
    Provides estimate count of master records in database.
    """

    id: str
    id_data_type: str
    name: str
    attribs: dict[str, Annotated[AttrSpecType, Field(discriminator="type")]]
    eid_estimate_count: NonNegativeInt

    @model_validator(mode="before")
    def _validate_attribs(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data
        if any(x not in data for x in ["id", "name"]):
            return data

        entity_spec = EntitySpec.model_validate(
            {
                "id": data["id"],
                "name": data["name"],
                "id_data_type": data.get("id_data_type", "string"),
                "snapshot": True,
            }
        )
        print(entity_spec)

        assert isinstance(data["attribs"], dict), "'attribs' must be a dictionary"
        with entity_context(entity_spec):
            data["attribs"] = {
                attr_id: AttrSpec(attr_id, spec) if not isinstance(spec, AttrSpecGeneric) else spec
                for attr_id, spec in data["attribs"].items()
            }

        return data


# This is necessary to allow for non-JSON-serializable types in the model
JsonVal = Annotated[Any, PlainSerializer(to_json_friendly, when_used="json")]

PlainVal = dict[str, JsonVal]
HistoryVal = list[dict[str, JsonVal]]

Dp3Val = Union[HistoryVal, PlainVal, JsonVal]

EntityEidMasterRecord = dict[str, Dp3Val]

SnapshotType = dict[str, Dp3Val]
EntityEidSnapshots = list[SnapshotType]


class EntityEidList(BaseModel):
    """List of entity eids and their data based on latest snapshot

    Includes timestamp of latest snapshot creation, count of returned documents
    and total count of documents available under specified filter.

    Data does not include history of observations attributes and timeseries.
    """

    time_created: Optional[datetime] = None
    count: int
    total_count: int
    data: EntityEidSnapshots


class EntityEidData(BaseModel):
    """Data of entity eid

    Includes all snapshots and master record.

    `empty` signalizes whether this eid includes any data.
    """

    empty: bool
    master_record: EntityEidMasterRecord
    snapshots: EntityEidSnapshots


class EntityEidAttrValueOrHistory(BaseModel):
    """Value and/or history of entity attribute for given eid

    Depends on attribute type:
    - plain: just (current) value
    - observations: (current) value and history stored in master record (optionally filtered)
    - timeseries: just history stored in master record (optionally filtered)
    """

    attr_type: AttrType
    current_value: JsonVal = None
    history: HistoryVal = []


class EntityEidAttrValue(BaseModel):
    """Value of entity attribute for given eid

    The value is fetched from master record.
    """

    value: JsonVal = None
