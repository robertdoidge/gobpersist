from __future__ import absolute_import
from .. import session
from .. import gob

import operator

class GobKVQuerent(session.Backend):
    """Abstract superclass (or pluggable back-end) for classes that
    aren't able to implement complex queries.
    """

    def _get_value(self, gob, arg):
        """Turn an argument into a value."""
        if isinstance(arg, tuple):
            # identifier
            value = gob
            for pathelem in arg:
                if not isinstance(value, gob.Gob):
                    raise ValueError("Could not understand identifier %s" \
                                         % repr(arg))
                if not isinstance(pathelem, field.Field):
                    pathelem = getattr(value, pathelem)
                if isinstance(pathelem, field.Foreign):
                    value = pathelem.value
                else:
                    value = pathelem
            if isinstance(value, gob.Gob):
                raise ValueError("Could not understand identifier %s" \
                                     % repr(arg))
            elif isinstance(value, schema.SchemaCollection):
                return value.list()
            else:
                return value
        else: # literal
            return arg


    def _apply_operator(self, gob, op, arg1, arg2):
        """Apply operator to the two arguments, taking quantifiers into account.
        """
        print "applying operator %s to %s and %s" % (repr(op),
                                                     repr(arg1),
                                                     repr(arg2))
        if isinstance(arg1, dict):
            if len(arg1) > 1:
                raise ValueError("Too many keys in quantifier")
            k, v = arg1.items()[0]
            v = self.get_value(gob, v)
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
                raise ValueError("Invalid key '%s' in quantifier" % k)
        elif isinstance(arg2, dict):
            if len(arg2) > 1:
                raise ValueError("Too many keys in quantifier")
            k, v = arg2.items()[0]
            v = self.get_value(gob, v)
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
                raise ValueError("Invalid key '%s' in quantifier" % k)
        else:
            return op(arg1, arg2)


    def _execute_query(self, gob, query):
        """Execute a query on an object, returning True if it matches
        the query and False otherwise."""
        print "executing %s on %s" % (repr(query), repr(gob))
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
        return True


    def query(self, path=None, path_range=None, query=None, retrieve=None,
              order=None, offset=None, limit=None):
        res = self.kv_query(path, path_range)
        ret = []
        current = -1
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
