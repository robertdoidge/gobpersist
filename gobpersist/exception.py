class NotFound(Exception):
    pass

class ConditionFailed(Exception):
    """Raised when a commit with conditions was attempted but those
    conditions could not be reconciled."""
    pass

class QueryError(Exception):
    """Raised when a malformed query is detected."""
    pass

class UnsupportedError(Exception):
    """Raised when the chosen back end does not support some
    operation."""
    pass

class Corruption(Exception):
    """Raised to indicate there is a condition in the database which
    conflicts with the semantics of the backend or schema which
    interfaces with it."""
    pass
