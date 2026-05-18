"""Shared hook type constants."""

from dp3.common.attrspec import AttrType

ATTR_TYPE_TO_ON_NEW_HOOK = {
    AttrType.PLAIN: "on_new_plain",
    AttrType.OBSERVATIONS: "on_new_observation",
    AttrType.TIMESERIES: "on_new_ts_chunk",
}
