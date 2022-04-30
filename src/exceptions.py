class Error(Exception):
    """Base class for exception in this module."""


class NoSettablePeriodError(Error):
    def __init__(self, message: str='') -> None:
        self.message = message
