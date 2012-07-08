# gobkvquerent.py - Abstract superclass for key-value stores
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
"""Support for classes that aren't able to implement complex queries
(that is, key--value stores).

.. codeauthor:: Evan Buswell <evan.buswell@accellion.com>
"""

import operator
import functools
import types

import gobpersist.session
import gobpersist.gob
import gobpersist.exception
import gobpersist.field

class GobKVQuerent(gobpersist.session.Backend):
    """Abstract superclass (or pluggable back end) for classes that
    aren't able to implement complex queries.
    """

    def _get_value_recursiter(self, gob, arg, path=None):
        """Turn an argument into a value, iterator version."""
        if isinstance(arg, tuple):
            # identifier
            if not path:
                path = list(arg)
            value = gob
            while path:
                pathelem = path.pop(0)
                if not isinstance(value, gobpersist.gob.Gob):
                    raise gobpersist.exception.QueryError("Could not understand " \
                                                              "identifier %s" \
                                                              % repr(arg))
                if isinstance(pathelem, gobpersist.field.Field):
                    pathelem = getattr(value, pathelem.name)
                else:
                    pathelem = getattr(value, pathelem)
                if isinstance(pathelem, gobpersist.field.ForeignObject):
                    value = pathelem.value
                elif isinstance(pathelem, gobpersist.field.ForeignCollection):
                    for item in pathelem.list():
                        for r in self._get_value_recursiter(item, arg, path):
                            yield r
                else:
                    value = pathelem
            if isinstance(value, gobpersist.gob.Gob):
                raise gobpersist.exception.QueryError("Could not understand " \
                                                          "identifier %s" \
                                                          % repr(arg))
            else:
                yield value
        else: # literal
            yield arg


    def _get_value(self, gob, arg):
        """Turn an argument into a value."""
        ret = []
        for value in self._get_value_recursiter(gob, arg):
            ret.append(value)
        if not ret:
            raise gobpersist.exception.QueryError("Could not understand " \
                                                      "identifier %s" \
                                                      % repr(arg))
        elif len(ret) > 1:
            return ret
        else:
            return ret[0]


    def _apply_operator(self, gob, op, arg1, arg2):
        """Apply operator to the two arguments, taking quantifiers
        into account.
        """
        #print "applying operator %s to %s and %s" % (repr(op),
        #                                             repr(arg1),
        #                                             repr(arg2))
        if isinstance(arg1, dict):
            if len(arg1) > 1:
                raise gobpersist.exception.QueryError("Too many keys in " \
                                                          "quantifier")
            k, v = arg1.items()[0]
            v = self._get_value_recursiter(gob, v)
            if k == 'any':
                for arg1 in v:
                    if self._apply_operator(gob, op, arg1, arg2):
                        return True
                return False
            elif k == 'all':
                for arg1 in v:
                    if not self._apply_operator(gob, op, arg1, arg2):
                        return False
                return True
            elif k == 'none':
                for arg1 in v:
                    if self._apply_operator(gob, op, arg1, arg2):
                        return False
                return True
            else:
                raise gobpersist.exception.QueryError("Invalid key '%s' in " \
                                                          "quantifier" % k)
        elif isinstance(arg2, dict):
            if len(arg2) > 1:
                raise gobpersist.exception.QueryError("Too many keys in " \
                                                          "quantifier")
            k, v = arg2.items()[0]
            v = self._get_value_recursiter(gob, v)
            if k == 'any':
                for arg2 in v:
                    if self._apply_operator(gob, op, arg1, arg2):
                        return True
                return False
            elif k == 'all':
                for arg2 in v:
                    if not self._apply_operator(gob, op, arg1, arg2):
                        return False
                return True
            elif k == 'none':
                for arg2 in v:
                    if self._apply_operator(gob, op, arg1, arg2):
                        return False
                return True
            else:
                raise gobpersist.exception.QueryError("Invalid key '%s' " \
                                                          "in quantifier" % k)
        else:
            return op(self._get_value(gob, arg1), self._get_value(gob, arg2))


    def _execute_query(self, gob, query):
        """Execute a query on an object, returning True if it matches
        the query and False otherwise."""
        #print "executing %s on %s" % (repr(query), repr(gob))
        for cmd, args in query.iteritems():
            if cmd in ('eq', 'ne', 'lt', 'gt', 'ge', 'le'):
                if len(args) < 2:
                    continue
                arg1 = args[0]
                for arg2 in args[1:]:
                    if not self._apply_operator(gob,
                                                getattr(operator, cmd),
                                                arg1, arg2):
                        return False
                    arg1 = arg2
            elif cmd == 'and':
                for subquery in args:
                    if not self._execute_query(gob, subquery):
                        return False
            elif cmd == 'or':
                for subquery in args:
                    if self._execute_query(gob, subquery):
                        continue
                return False
            elif cmd in ('nor', 'not'):
                for subquery in args:
                    if self._execute_query(gob, subquery):
                        return False
            else:
                raise gobpersist.exception.QueryError("Unknown query element " \
                                                          "%s" % repr(cmd))
        return True
        

    def query(self, cls, key=None, key_range=None, query=None, retrieve=None,
              order=None, offset=None, limit=None):
        res = self.kv_query(cls, key, key_range)
        ret = []
        current = -1
        if order is not None:
            def order_cmp(a, b):
                for ordering in order:
                    if not isinstance(ordering, dict) or not len(ordering) == 1:
                        raise ValueError("Invalid ordering: %s" % repr(ordering))
                    key, ordering = ordering.items()[0]
                    if isinstance(ordering, gobpersist.field.Field):
                        ordering = ordering._name
                    if not isinstance(ordering, tuple):
                        ordering = (ordering,)
                    res = cmp(self._get_value(a, ordering), self._get_value(b, ordering))
                    if res == 0:
                        continue
                    if key == 'asc':
                        return res
                    elif key == 'desc':
                        return -res
                    else:
                        raise ValueError("Invalid key '%s' in ordering %s" \
                                             % (key, repr(ordering)))
            res.sort(key=functools.cmp_to_key(order_cmp))
        for item in res:
            if query is not None and not self._execute_query(item, query):
                continue
            current += 1
            if offset is not None and current < offset:
                continue
            ret.append(item)
            if limit is not None and len(ret) == limit:
                return ret
        return ret
