from datetime import datetime
from typing import Any, Optional, Union

from pydantic import BaseModel, root_validator, validator
from pydantic.error_wrappers import ValidationError

from dp3.common.datapoint import DataPointBase
from dp3.common.entityspec import EntitySpec


class Task(BaseModel):
    """Task

    Contains single task to be pushed to TaskQueue and processed.
    """

    # Attribute spec just for internal validation. Discarded after that.
    attr_spec: Optional[dict[str, dict[str, Union[EntitySpec, dict[str, Any]]]]] = None

    etype: str
    ekey: str
    data_points: list[DataPointBase] = []
    tags: list[Any] = []
    ttl_token: Optional[datetime] = None

    @validator("etype")
    def validate_etype(cls, v, values):
        if "attr_spec" in values:
            assert v in values["attr_spec"], f"Invalid etype '{self.etype}'"
        return v

    @validator("data_points", pre=True, each_item=True)
    def instanciate_dps(cls, v, values):
        if "attr_spec" in values and values["attr_spec"]:
            # Convert `DataPointBase` instances back to dicts
            if isinstance(v, DataPointBase):
                v = v.dict()

            etype = v.get("etype")
            attr = v.get("attr")

            # Fetch datapoint model
            try:
                dp_model = values["attr_spec"][etype]["attribs"][attr]._dp_model
            except (KeyError, TypeError) as e:
                raise ValueError(f"Attribute '{attr}' not found in entity '{etype}'") from e

            # Parse datapoint using model from attribute specification
            try:
                return dp_model.parse_obj(v)
            except ValidationError as e:
                raise ValueError(e) from e

        return v

    @validator("data_points", each_item=True)
    def validate_data_points(cls, v, values):
        if "etype" in values and "ekey" in values:
            assert (
                v.etype == values["etype"]
            ), f"Task's etype '{values['etype']}' doesn't match datapoint's etype '{v.etype}'"
            assert (
                v.eid == values["ekey"]
            ), f"Task's ekey '{values['ekey']}' doesn't match datapoint's eid '{v.eid}'"
        return v

    @root_validator
    def discard_attr_spec(cls, values):
        # This is run at the end of validation.
        # Discard attribute specification.
        del values["attr_spec"]
        return values
