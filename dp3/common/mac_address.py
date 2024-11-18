from typing import Any, Union

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema, core_schema


class MACAddress:
    """Represents a MAC Address.

    Can be initialized from colon or comma separated string, or from raw bytes.
    """

    def __init__(self, mac: Union[bytes, str, "MACAddress"]):
        if isinstance(mac, self.__class__):
            mac = mac.mac  # type: ignore
        if not isinstance(mac, bytes) or len(mac) != 6:
            mac = self._parse_mac(mac)

        self.mac: bytes = mac

    @classmethod
    def _validate(cls, value: Any) -> "MACAddress":
        return cls(value)

    @classmethod
    def _serialize(cls, value: "MACAddress", info: Any) -> Any:
        if info.mode == "json":
            return str(value)
        return value

    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema:
        base_schema = core_schema.no_info_after_validator_function(
            cls._validate, handler(Union[str, bytes])
        )

        python_schema = core_schema.union_schema(
            [
                core_schema.is_instance_schema(cls=cls),
                base_schema,
            ],
        )

        return core_schema.json_or_python_schema(
            json_schema=base_schema,
            python_schema=python_schema,
            serialization=core_schema.plain_serializer_function_ser_schema(
                cls._serialize, info_arg=True
            ),
        )

    @staticmethod
    def _parse_mac(mac: Union[bytes, str]) -> bytes:
        if isinstance(mac, str):
            mac = mac.encode()
        if not isinstance(mac, bytes):
            raise TypeError(f"Expected str or bytes, got {type(mac)}")

        if len(mac) != 17:
            raise ValueError(f"Invalid MAC address length, {len(mac)} != 17")
        if mac[2] not in [ord(":"), ord("-")]:
            print(mac[2])
            raise ValueError("Invalid MAC address format, expected separator ':' or '-'")

        mac_bytes = bytearray(6)
        for i in range(0, 18, 3):
            mac_bytes[i // 3] = int(mac[i : i + 2], 16)
            if i == 15:
                break
            if mac[i + 2] != mac[2]:
                raise ValueError(
                    f"Invalid MAC address format at pos {i + 2}, "
                    f"expected separator '{chr(mac[2])}' but got '{chr(mac[i + 2])}'"
                )
        return bytes(mac_bytes)

    def __str__(self):
        return ":".join(f"{b:02x}" for b in self.mac)

    def __repr__(self):
        return f"{self.__class__.__name__}({self})"

    def __eq__(self, other):
        return isinstance(other, MACAddress) and self.mac == other.mac

    def __hash__(self):
        return hash(self.mac)

    def __bytes__(self):
        return self.mac

    @property
    def packed(self):
        return self.mac
