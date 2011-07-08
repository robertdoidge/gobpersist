from __future__ import absolute_import
# Moved to the middle of a function to avoid cyclical dependency
# from . import schema
import re
from copy import copy as base_clone
import operator
import datetime
import uuid
import iso8601

class Field(object):
    """An abstract base class for defining a field type."""

    def __init__(self, null=False, unique=False, primary_key=False, revision_tag=False, name=None, default=None, modifiable=True):
        self._key = None
        self.dirty = True
        self.immutable = False
        self.null = null
        self.unique = unique
        self.default = default
        self.primary_key = primary_key
        self.revision_tag = revision_tag
        self.modifiable = modifiable
        self.name = name
        self.value = None
        self.has_value = False

    def prepare_persist(self):
        if not self.has_value:
            self.set(self.default() if callable(self.default) else self.default)

    def trip_set(self):
        assert not self.immutable, "If hash(field) is called, field is marked as immutable"
        self.dirty = True
        self.has_value = True
        if self.instance is not None:
            self.instance.dirty = True

    def reset_state(self):
        self.immutable = False
        self.dirty = False

    def mark_clean(self):
        self.dirty = False

    def _set(self, value):
        self.value = value

    def set(self, value):
        self.validate(value)
        self.trip_set()
        self._set(value)

    def __set__(self, instance, value):
        instance.__dict__[self._key].set(value)

    def __get__(self, instance, owner):
        if instance is not None:
            return instance.__dict__[self._key]
        else:
            return self

    def __delete__(self, instance):
        instance.__dict__[self._key].set(None)

    def validate(self, value):
        assert self.modifiable if self.instance.persisted else True, "Field is not modifiable"
        if value is None and not self.null:
            raise ValueError("'None' not allowed for field '%s'" % self.name)

    def clone(self):
        ret = base_clone(self)
        ret.immutable = False
        return ret

    def __repr__(self):
        return repr(self.value)
    def __str__(self):
        return str(self.value)
    def __lt__(self, other):
        return self.value < other
    def __le__(self, other):
        return self.value <= other
    def __eq__(self, other):
        return self.value == other
    def __ne__(self, other):
        return self.value != other
    def __gt__(self, other):
        return self.value > other
    def __ge__(self, other):
        return self.value >= other
    def __cmp__(self, other):
        return cmp(self.value, other)
    def __hash__(self):
        self.immutable = True
        return hash(self.value)
    def __nonzero__(self):
        return bool(self.value)
    def __instancecheck__(self, cls):
        return isinstance(self, cls) or isinstance(self.value, cls)
    def __subclasscheck__(self, cls):
        return issubclass(self, cls) or issubclass(self.value, cls)

class ForeignCollection(object):
    """A field to represent a foreign collection or object"""

    def __init__(self, foreign_class, local_key, foreign_key, name=None, many=True):
        self.foreign_class = foreign_class
        self.foreign_key = foreign_key
        self.local_key = local_key
        self.many = many
        self.name = name

    def __get__(self, instance, owner):
        if instance is None:
            return self
        elif self._key in instance.__dict__:
            return instance.__dict__[self._key]
        else:
            if self.many:
                # The import dragons will keep you from doing something more obvious.
                from . import schema
                local_key = getattr(instance, self.local_key)
                if self.name is not None:
                    ret = schema.SchemaCollection(instance.session, (instance.__class__, instance.primary_key, self),
                                                  autoset={self.foreign_key : local_key})
                else:
                    ret = schema.SchemaCollection(instance.session, (self.foreign_class,),
                                                  sticky={'eq': [(self.foreign_key,), local_key]},
                                                  autoset={self.foreign_key : local_key})
                return ret
            else:
                if self.name is not None:
                    ret = instance.session.query((instance.__class__, instance.primary_key, self))
                else:
                    ret = instance.session.query((self.foreign_class,), {'eq': [(self.foreign_key,), getattr(instance, self.local_key)]})
                if ret:
                    instance.__dict__[self._key] = ret[0]
                    return ret[0]
                else:
                    return None

class BooleanField(Field):
    """A field to represent a boolean value."""

    def validate(self, value):
        super(BooleanField, self).validate(value)
        if not isinstance(value, bool):
            raise TypeError("'%s' object is not bool" % type(value))

class DateTimeField(Field):
    """A field to represent a point in time."""

    def _set(self, value):
        if isinstance(value, (unicode, str)):
            value = iso8601.parse_datetime(value)
        super(DateTimeField, self)._set(value)

    def validate(self, value):
        super(DateTimeField, self).validate(value)
        if isinstance(value, (unicode, str)):
            if iso8601.ISO8601_RE.match(value) is None:
                raise ValueError("'%s' does not appear to be an ISO 8601 string" % value)
        elif not isinstance(value, datetime.datetime) and value is not None:
            raise TypeError("'%s' object is not datetime" % type(value))

class StringField(Field):
    """A field to represent string data."""

    def __init__(self, encoding='binary', max_length=None, allow_empty=True, validate = lambda x: True, *args, **kwargs):
        self.max_length = max_length
        self.allow_empty = True
        self.validate_extra = validate
        self.encoding = encoding
        super(StringField, self).__init__(*args, **kwargs)

    def _set(self, value):
        if value is None:
            self.value = None
            self.value_encoded = None
            self.value_decoded = None
            super(StringField, self)._set(None)
            return
        if self.encoding == 'binary':
            if isinstance(value, unicode):
                self.value_encoded = value.encode('utf-8')
                self.value_decoded = value
            else:
                self.value_decoded = None
                self.value_encoded = value
            super(StringField, self)._set(self.value_encoded)
        else:
            if isinstance(value, unicode):
                self.value_encoded = value.encode(self.encoding)
                self.value_decoded = value
            else:
                self.value_decoded = value.decode(self.encoding)
                self.value_encoded = value
            super(StringField, self)._set(self.value_decoded)

    def validate(self, value):
        super(StringField, self).validate(value)
        if value is None:
            return
        if not isinstance(value, (unicode, str)):
            raise TypeError("'%s' object is neither unicode nor bytes" % type(value))
        if self.max_length is not None \
                and len(value) > self.max_length:
            raise ValueError("'%s' longer than maximum length" % value)
        if not self.allow_empty and value == "":
            raise ValueError("Empty string not permitted")
        if not self.validate_extra(value):
            raise ValueError("'%s' failed validation %s" % (value, repr(self.validate_extra)))

    def __repr__(self):
        return repr(self.value)
    def __str__(self):
        return self.value if self.value is not None else str(None)
    def __unicode__(self):
        return self.value_decoded if self.value_decoded is not None else unicode(self.value)
    def decode(self, *args, **kwargs):
        return self.value_decoded if self.value_decoded is not None else self.value.decode(*args, **kwargs)
    def encode(self, encoding=None, *args, **kwargs):
        if encoding == self.encoding:
            return self.value_encoded
        else:
            return self.value_decoded.encode(encoding, *args, **kwargs) if self.value_decoded is not None \
                else self.value.encode(encoding, *args, **kwargs)

    def __len__(self):
        return len(self.value)
    def __getitem__(self, key):
        return self.value[key]
    def __iter__(self):
        return self.value.__iter__()
    def __reversed__(self):
        return reversed(self.value)
    def __contains__(self, item):
        return item in self.value
    def __add__(self, other):
        return self.value + other
    def __radd__(self, other):
        return other + self.value
    def __iadd__(self, other):
        self.set(self.value + other)
    def __mul__(self, other):
        return self.value * other
    def __rmul__(self, other):
        return other * self.value
    def __imul__(self, other):
        self.set(self.value * other)
    

    def capitalize(self):
        return self.value.capitalize
    def center(self, *args, **kwargs):
        return self.value.center(*args, **kwargs)
    def count(self, *args, **kwargs):
        return self.value.cound(*args, **kwargs)
    def endswith(self, *args, **kwargs):
        return self.value.endswith(*args, **kwargs)
    def expandtabs(self, *args, **kwargs):
        return self.value.expandtabs(*args, **kwargs)
    def find(self, *args, **kwargs):
        return self.value.find(*args, **kwargs)
    def format(self, *args, **kwargs):
        return self.value.format(*args, **kwargs)
    def index(self, *args, **kwargs):
        return self.value.index(*args, **kwargs)
    def isalnum(self):
        return self.value.isalnum()
    def isalpha(self):
        return self.value.isalpha()
    def isdigit(self):
        return self.value.isdigit()
    def islower(self):
        return self.value.islower()
    def isspace(self):
        return self.value.isspace()
    def istitle(self):
        return self.value.istitle()
    def isupper(self):
        return self.value.isupper()
    def join(self, iterable):
        return self.value.join(iterable)
    def ljust(self, *args, **kwargs):
        return self.value.ljust(*args, **kwargs)
    def lower(self):
        return self.value.lower()
    def lstrip(self, *args, **kwargs):
        return self.value.lstrip(*args, **kwargs)
    def partition(self, sep):
        return self.value.partition(sep)
    def replace(self, *args, **kwargs):
        return self.value.replace(*args, **kwargs)
    def rfind(self, *args, **kwargs):
        return self.value.rfind(*args, **kwargs)
    def rindex(self, *args, **kwargs):
        return self.value.rindex(*args, **kwargs)
    def rjust(self, *args, **kwargs):
        return self.value.rjust(*args, **kwargs)
    def rpartition(self, sep):
        return self.value.rpartition(sep)
    def rsplit(self, *args, **kwargs):
        return self.value.rsplit(*args, **kwargs)
    def rstrip(self, *args, **kwargs):
        return self.value.rstrip(*args, **kwargs)
    def split(self, *args, **kwargs):
        return self.value.split(*args, **kwargs)
    def splitlines(self, *args, **kwargs):
        return self.value.splitlines(*args, **kwargs)
    def startswith(self, *args, **kwargs):
        return self.value.startswith(*args, **kwargs)
    def strip(self, *args, **kwargs):
        return self.value.strip(*args, **kwargs)
    def swapcase(self):
        return self.value.swapcase()
    def title(self):
        return self.value.title()
    def translate(self, *args, **kwargs):
        return self.value.translate(*args, **kwargs)
    def upper(self):
        return self.value.upper()
    def zfill(self, width):
        return self.value.zfill(self, width)

    # These intentionally fail in the case where value_decoded is None (binary data)
    def isnumeric(self):
        return self.value_decoded.isnumeric() if self.value_decoded is not None \
            else self.value.isnumeric()
    def isdecimal(self):
        return self.value_decoded.isdecimal() if self.value_decoded is not None \
            else self.value.isdecimal()

class NumericField(Field):
    """Abstract superclass for RealField and IntegerField."""

    def __add__(self, other):
        return self.value + other
    def __sub__(self, other):
        return self.value - other
    def __mul__(self, other):
        return self.value * other
    def __div__(self, other):
        return self.value / other
    def __truediv__(self, other):
        return self.value / other
    def __floordiv__(self, other):
        return self.value // other
    def __mod__(self, other):
        return self.value % other
    def __divmod__(self, other):
        return divmod(self.value, other)
    def __pow__(self, *args):
        return pow(self.value, *args)
    def __lshift__(self, other):
        return self.value << other
    def __rshift__(self, other):
        return self.value >> other
    def __and__(self, other):
        return self.value & other
    def __xor__(self, other):
        return self.value ^ other
    def __or__(self, other):
        return self.value | other

    def __radd__(self, other):
        return other + self.value
    def __rsub__(self, other):
        return other - self.value
    def __rmul__(self, other):
        return other * self.value
    def __rdiv__(self, other):
        return other / self.value
    def __rtruediv__(self, other):
        return other / self.value
    def __rfloordiv__(self, other):
        return other // self.value
    def __rmod__(self, other):
        return other % self.value
    def __rdivmod__(self, other):
        return divmod(other, self.value)
    def __rpow__(self, other):
        return pow(other, self.value)
    def __rlshift__(self, other):
        return other << self.value
    def __rrshift__(self, other):
        return other >> self.value
    def __rand__(self, other):
        return other & self.value
    def __rxor__(self, other):
        return other ^ self.value
    def __ror__(self, other):
        return other | self.value
    
    def __iadd__(self, other):
        self.set(self.value + other)
    def __isub__(self, other):
        self.set(self.value - other)
    def __imul__(self, other):
        self.set(self.value * other)
    def __idiv__(self, other):
        self.set(self.value / other)
    def __itruediv__(self, other):
        self.set(self.value / other)
    def __ifloordiv__(self, other):
        self.set(self.value // other)
    def __imod__(self, other):
        self.set(self.value % other)
    def __ipow__(self, *args):
        self.set(pow(self.value, *args))
    def __ilshift__(self, other):
        self.set(self.value << other)
    def __irshift__(self, other):
        self.set(self.value >> other)
    def __iand__(self, other):
        self.set(self.value & other)
    def __ixor__(self, other):
        self.set(self.value ^ other)
    def __ior__(self, other):
        self.set(self.value | other)

    def __neg__(self):
        return -self.value
    def __pos__(self):
        return +self.value
    def __abs__(self):
        return abs(self.value)
    def __invert__(self):
        return ~self.value
    def __complex__(self):
        return complex(self.value)
    def __int__(self):
        return int(self.value)
    def __long__(self):
        return long(self.value)
    def __float__(self):
        return float(self.value)

    def __oct__(self):
        return oct(self.value)
    def __hex__(self):
        return hex(self.value)
            
    def __index__(self):
        return operator.index(self.value)
    def __coerce__(self, other):
        return coerce(self.value, other)

class IntegerField(NumericField):
    """Field representing all integer types."""

    def __init__(self, unsigned=False, precision=32, *args, **kwargs):
        self.precision = precision
        self.unsigned = unsigned
        if precision == 64:
            self.maximum = 18446744073709551615 if unsigned else 9223372036854775807
            self.minimum = 0 if unsigned else -9223372036854775808
        elif precision == 32:
            self.maximum = 4294967295 if unsigned else 2147483647
            self.minimum = 0 if unsigned else -2147483648
        elif precision == 16:
            self.maximum = 65535 if unsigned else 32767
            self.minimum = 0 if unsigned else -32768
        elif precision == 8:
            self.maximum = 255 if unsigned else 127
            self.minimum = 0 if unsigned else -128
        else:
            raise ValueError("precision must be 8, 16, 32, or 64")
        super(IntegerField, self).__init__(*args, **kwargs)

    def validate(self, value):
        super(IntegerField, self).validate(value)
        if not isinstance(value, (int,long)):
            raise TypeError("'%s' object is not int" % type(value))
        if value > self.maximum or value < self.minimum:
            raise ValueError("'%s' out of range" % value)

    def _set(self, value):
        if isinstance(value, int) and (self.precision == 64 or (self.precision == 32 and self.unsigned)):
            value = long(value)
        super(IntegerField, self)._set(value)

    def bit_length(self):
        return self.value.bit_length()

class RealField(NumericField):
    """Field representing all floating point types."""

    def __init__(self, precision='double', *args, **kwargs):
        if precision not in ['half', 'single', 'double', 'quad']:
            raise ValueError("precision must be one of 'half', 'single', 'double', or 'quad'")
        self.precision = precision
        super(RealField, self).__init__(*args, **kwargs)

    def validate(self, value):
        super(RealField, self).validate(value)
        if not isinstance(value, float):
            raise TypeError("'%s' object is not float" % type(value))

    def as_integer_ratio(self):
        return self.value.as_integer_ratio()
    def is_integer(self):
        return self.value.is_integer()
    def hex(self):
        return self.value.hex()

class EnumField(StringField):
    """A field to represent enumerations."""

    def __init__(self, choices, *args, **kwargs):
        self.choices = choices
        super(EnumField, self).__init__(*args, **kwargs)

    def validate(self, value):
        super(EnumField, self).validate(value)
        if value not in self.choices:
            raise ValueError("'%s' not in choices: %s" % (value, self.choices.join(", ")))

class UUIDField(StringField):
    """A field to represent UUIDs."""
    validate_re = re.compile('\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')

    def __init__(self, *args, **kwargs):
        if 'encoding' not in kwargs:
            kwargs['encoding'] = 'us-ascii'
        if 'default' not in kwargs:
            kwargs['default'] = lambda: str(uuid.uuid4())
        super(UUIDField, self).__init__(*args, **kwargs)

    def validate(self, value):
        super(UUIDField, self).validate(value)
        if value is None:
            return
        if not self.validate_re.match(value):
            raise ValueError("'%s' is not a valid UUID" % str(value))

class MultiField(Field):
    """An abstract field representing multiple uniformly-typed values."""

    def _element_to_field(self, element):
        return self.field.clone.set(element.value if isinstance(element, Field) else element)

    def __init__(self, element_type, *args, **kwargs):
        self.field = element_type
        super(MultiField, self).__init__(*args, **kwargs)

    def __iter__(self):
        return self.value.__iter__()
    def __len__(self):
        return len(self.value)
    def __contains__(self, elem):
        return elem in self.value
    def reset_state(self):
        for element in self.value:
            element.reset_state()
        super(MultiField, self).reset_state()
    def mark_clean(self):
        for element in self.value:
            element.mark_clean()
        super(MultiField, self).mark_clean()
    def prepare_persist(self):
        super(MultiField, self).prepare_persist()
        for element in self.value:
            element.prepare_persist()

class ListField(MultiField):
    """A field to represent a list of other fields of a uniform type."""

    def clone(self):
        copy = super(ListField, self).clone()
        copy.value = [element.clone() for element in copy.value]
        return copy

    def _set(self, value):
        newvalue = [self._element_to_field(element) for element in value]
        super(ListField, self)._set(newvalue)

    def __getitem__(self, key):
        return self.value[key]
    def __add__(self, other):
        return self.value + other
    def __radd__(self, other):
        return other + self.value
    def __mul__(self, other):
        return self.value * other
    def __rmul__(self, other):
        return other * self.value
    def __reversed__(self):
        return reversed(self.value)
    def count(self, elem):
        return self.value.count(elem)
    def index(self, *args, **kwargs):
        return self.value.index(*args, **kwargs)

    def __setitem__(self, key, value):
        self.trip_set()
        if isinstance(key, slice):
            self.value[key] = [self._element_to_field(element) for element in value]
        else:
            self.value[key] = self._element_to_field(value)
    def __delitem__(self, key):
        self.trip_set()
        del self.value[key]
    def __iadd__(self, other):
        self.trip_set()
        try:
            self.value += [self._element_to_field(element) for element in other]
        except TypeError:
            self.value += self._element_to_field(other)
    def __imul__(self, other):
        self.trip_set()
        if self.value is None or not isinstance(other, int):
            # generate appropriate error
            self.value *= other
        if other <= 0:
            self.value = []
        else:
            currentvalue = base_clone(value)
            for dummy in xrange(1, other):
                self.extend(currentvalue)
    def reverse(self):
        self.trip_set()
        self.value.reverse()
    def sort(self, *args, **kwargs):
        self.trip_set()
        self.value.sort(*args, **kwargs)
    def extend(elems):
        self.trip_set()
        self.value.extend([self._element_to_field(element) for element in elems])
    def insert(self, i, elem):
        self.trip_set()
        self.value.insert(i, self._element_to_field(elem))
    def pop(self):
        self.trip_set()
        return self.value.pop()
    def remove(self, elem):
        self.trip_set()
        self.value.remove(elem)
    def append(self, elem):
        self.trip_set()
        self.value.append(self._element_to_field(elem))

    def __hash__(self):
        self.value = tuple(self.value)
        return super(ListField, self).__hash__(self)

class SetField(MultiField):
    """A field to represent a set of other fields of a uniform type."""

    def clone(self):
        copy = super(SetField, self).clone()
        copy.value = set([element.clone() for element in copy.value])
        return copy

    def _set(self, instance, value):
        newvalue = set([self._element_to_field(element) for element in value])
        super(SetField, self)._set(instance, newvalue)

    def isdisjoint(self, other):
        return self.value.isdisjoint(other)
    def issubset(self, other):
        return self.value.issubset(other)
    def issuperset(self, other):
        return self.value.issuperset(other)
    def union(self, *args):
        return self.value.union(*args)
    def __or__(self, other):
        return self.value | other
    def intersection(self, *args):
        return self.value.intersection(*args)
    def __and__(self, other):
        return self.value & other
    def difference(self, *args):
        return self.value.difference(*args)
    def __sub__(self, other):
        return self.value - other
    def symmetric_difference(other):
        return self.value.symmetric_difference(other)
    def __xor__(self, other):
        return self.value ^ other
    def copy(self):
        return frozenset() + self.value

    def update(self, *others):
        self.trip_set()
        self.value.update([[self._element_to_field(element) for element in other] for other in others])
    def __ior__(self, other):
        self.trip_set()
        if not isinstance(other, (set, frozenset)):
            # trigger appropriate error message
            self.value |= other
        self.value |= frozenset([self._element_to_field(element) for element in other])
    def intersection_update(self, *others):
        self.trip_set()
        self.value.intersection_update([[self._element_to_field(element) for element in other] for other in others])
    def __iand__(self, other):
        self.trip_set()
        if not isinstance(other, (set, frozenset)):
            # trigger appropriate error message
            self.value &= other
        self.value &= frozenset([self._element_to_field(element) for element in other])
    def difference_update(self, *others):
        self.trip_set()
        self.value.difference_update([[self._element_to_field(element) for element in other] for other in others])
    def __isub__(self, other):
        self.trip_set()
        if not isinstance(other, (set, frozenset)):
            # trigger appropriate error message
            self.value -= other
        self.value -= frozenset([self._element_to_field(element) for element in other])
    def symmetric_difference_update(self, *others):
        self.trip_set()
        self.value.symmetric_difference_update([[self._element_to_field(element) for element in other] for other in others])
    def __ixor__(self, other):
        self.trip_set()
        if not isinstance(other, (set, frozenset)):
            # trigger appropriate error message
            self.value ^= other
        self.value ^= frozenset([self._element_to_field(element) for element in other])
    def add(self, elem):
        self.trip_set()
        self.value.add(self._element_to_field(elem))
    def remove(self, elem):
        self.trip_set()
        self.value.remove(elem)
    def discard(self, elem):
        self.trip_set()
        self.value.discard(elem)
    def pop(self, elem):
        self.trip_set()
        return self.value.pop()
    def clear(self):
        self.trip_set()
        return self.value.clear()

    def __hash__(self):
        self.value = frozenset(self.value)
        return super(ListField, self).__hash__(self)
