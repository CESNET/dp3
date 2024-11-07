from ipaddress import IPv4Address, IPv6Address

from bson import Binary
from bson.binary import USER_DEFINED_SUBTYPE
from bson.codec_options import CodecOptions, TypeDecoder, TypeRegistry

from dp3.common.mac_address import MACAddress

BSON_OBJECT_TOO_LARGE = 10334

BSON_IPV4_SUBTYPE = USER_DEFINED_SUBTYPE + 1
BSON_IPV6_SUBTYPE = BSON_IPV4_SUBTYPE + 1
BSON_MAC_SUBTYPE = BSON_IPV6_SUBTYPE + 1


def fallback_encoder(value):
    if isinstance(value, IPv4Address):
        return Binary(value.packed, BSON_IPV4_SUBTYPE)
    if isinstance(value, IPv6Address):
        return Binary(value.packed, BSON_IPV6_SUBTYPE)
    if isinstance(value, MACAddress):
        return Binary(value.mac, BSON_MAC_SUBTYPE)
    return value


class DP3BinaryDecoder(TypeDecoder):
    bson_type = Binary

    def transform_bson(self, value):
        if value.subtype == BSON_IPV4_SUBTYPE:
            return IPv4Address(value)
        if value.subtype == BSON_IPV6_SUBTYPE:
            return IPv6Address(value)
        if value.subtype == BSON_MAC_SUBTYPE:
            return MACAddress(value)
        return value


def get_codec_options():
    tr = TypeRegistry([DP3BinaryDecoder()], fallback_encoder=fallback_encoder)
    return CodecOptions(type_registry=tr)
