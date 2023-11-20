class SharedFlag:
    """A bool wrapper that can be shared between multiple objects"""

    def __init__(self, flag: bool = False, banner: str = ""):
        self._flag = flag
        self._banner = banner

    def set(self, flag: bool = True):
        """Set the flag to `flag`. True by default."""
        self._flag = flag

    def unset(self):
        """Unset the flag."""
        self._flag = False

    def isset(self):
        """Check if the flag is set."""
        return self._flag

    def __repr__(self):
        if self._banner:
            return f"SharedFlag({repr(self._banner)}, {repr(self._flag)})"
        return f"SharedFlag({repr(self._flag)})"

    def __str__(self):
        return str(self._flag)
