class GobKVQuerent(object):
    def __init__(self, backend=None):
        self.backend = backend
        # Allow caller to set session
        self.session = None

    def __getattr__(self, name):
        return getattr(self.backend, name)

    def _get_value(gob, arg):
        """Turn an argument into a value."""
        if isinstance(arg, tuple):
            value = gob
            for pathelem in arg:
                if isinstance(value, schema.SchemaCollection):
                    value = value.get(pathelem)
                else:
                    value = getattr(value, pathelem)
            if isinstance(value, schema.SchemaCollection):
                return value.list()
            else:
                return value
        else: # literal
            return arg

    def _apply_operator(gob, op, arg1, arg2):
        """Apply operator to the two arguments, taking quantifiers into account."""
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
        else:
            return op(arg1, arg2)

    def _execute_query(gob, query):
        for cmd, args in query:
            if cmd in ('eq', 'ne', 'lt', 'gt', 'ge', 'le'):
                if len(args) < 2:
                    continue
                arg1 = args[0]
                for arg2 in args[1:]:
                    if not self._apply_operator(gob, getattr(operator, cmd), arg1, arg2):
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

    def query(self, path, query, retrieve, offset, limit):
        res = self.session._query(path, {}, retrieve)
        ret = []
        current = -1
        for item in res:
            if not self._execute_query(item, query):
                continue
            current += 1
            if offset is not None and current < offset:
                continue
            ret.append(item)
            if limit is not None and len(ret) == limit:
                return ret
        return ret
