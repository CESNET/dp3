from dp3.api.internal.config import MODEL_SPEC
from dp3.common.datapoint import DataPointBase


def api_to_dp3_datapoint(api_dp_values: dict) -> DataPointBase:
    """Converts API datapoint values to DP3 datapoint

    If etype-attr pair doesn't exist in DP3 config, raises `ValueError`.
    If values are not valid, raises pydantic's ValidationError.
    """
    etype = api_dp_values["type"]
    attr = api_dp_values["attr"]

    # Convert to DP3 datapoint format
    dp3_dp_values = api_dp_values
    dp3_dp_values["etype"] = etype
    dp3_dp_values["eid"] = api_dp_values["id"]

    # Get attribute-specific model
    try:
        model = MODEL_SPEC.attr(etype, attr).dp_model
    except KeyError as e:
        raise ValueError(f"Combination of type '{etype}' and attr '{attr}' doesn't exist") from e

    # Parse using the model
    # This may raise pydantic's ValidationError, but that's intensional (to get
    # a JSON-serializable trace as a response from API).
    return model.model_validate(dp3_dp_values)
