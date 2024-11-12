from collections.abc import Iterator
from contextlib import contextmanager
from contextvars import ContextVar

_init_attr_spec_context_var = ContextVar("_init_attr_spec_context_var", default=None)


@contextmanager
def entity_context(self_spec, entities: dict) -> Iterator[None]:
    """Context manager for AttrSpec initialization."""
    token = _init_attr_spec_context_var.set({"self": self_spec, "entities": entities})
    try:
        yield
    finally:
        _init_attr_spec_context_var.reset(token)


def get_entity_context() -> dict:
    """Get entity spec context."""
    cxt = _init_attr_spec_context_var.get()
    if cxt is None or not isinstance(cxt, dict):
        raise ValueError("Entity spec context is not set")
    return cxt
