# exception.py - exceptions used in gobpersist
# Copyright (C) 2012 Accellion, Inc.
#
# This library is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as
# published by the Free Software Foundation; version 2.1.
#
# This library is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301 USA
"""Exceptions used by gobpersist.

Only some of these make it to the outside world.

.. moduleauthor:: Evan Buswell <evan.buswell@accellion.com>
"""

class NotFound(Exception):
    """Raised when a query key or subkey is not found.

    This condition is different than an empty result.  For instance,
    if one queries on a key, but that key is missing, a ``NotFound``
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
