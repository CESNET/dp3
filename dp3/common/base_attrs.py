from .attrspec import AttrSpec

BASE_ATTRIBS = {
    '_lru': AttrSpec("_lru", {'name': "last regular update", 'data_type': "time"}),
    '_ttl': AttrSpec("_ttl", {'name': "TTL tokens", 'data_type': "json"}),
}