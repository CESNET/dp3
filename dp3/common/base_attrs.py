from .attrspec import AttrSpec

BASE_ATTRIBS = {
    "_lru": AttrSpec("_lru", {"name": "last regular update", "type": "plain", "data_type": "time"}),
    "_ttl": AttrSpec("_ttl", {"name": "TTL tokens", "type": "plain", "data_type": "json"}),
}
