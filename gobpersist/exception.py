class NotFound(Exception):
    """Raised when a query key or subkey is not found.

    This condition is different than an empty result.  For instance,
    if one queries on a key, but that key is missing, a NotFound
    exception is thrown, but if one queries on a key, and that key is
    present, though no objects are stored in it, and empty list is
    returned.
    """
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

class Deadlock(Exception):
    """Raised to indicate that a required lock cannot be obtained."""
    pass
