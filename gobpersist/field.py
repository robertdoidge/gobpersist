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
    instance = None

    def __init__(self, null=False, unique=False, primary_key=False,
                 revision_tag=False, name=None, default=None, modifiable=True):
        
        self.null = null
        """Whether or not the field may be None."""

        self.unique = unique
        """Indicates that this field must be unique."""

        self.default = default
        """Callable or scalar indicating the default value for this field."""

        self.primary_key = primary_key
        """Indicates whether or not this field is the primary key for this
        object."""

        self.revision_tag = revision_tag
        """Indicates this field is a revision tag.

        A revision tag must be unchanged between reading the object and
        updating/removing the object.
        """

        self.modifiable = modifiable
        """Indicates whether or not this field can be modified for update."""

        self.name = name
        """Hints at the name of this field, if different from the variable name
        that refers to it."""

        self._key = None
        """The key used to store data on the instance."""

        self.dirty = True
        """Whether or not this field has been altered."""

        self.immutable = False
        """Indicates that this field has become immutable.
        
        This will be set True when the hash() function is called on this object,
        whether explicitly or through the use of this field as a dictionary key
        or a member of a set.
        """

        self.key = None
        """The name of the variable that refers to this field."""

        self.value = None
        """The actual value of this field."""

        self.has_value = False
        """Whether or not a value has been set.

        This can be used to distinguish between explicit None and implicit None.
        """

        self.instance = None
        """The Gob instance for this instance of Field.

        None, if this represents an abstract field or a field on a class.
        """

    def prepare_persist(self):
        """Prepares this field to be persisted."""
        if not self.instance.persisted and not self.has_value:
            self.set(self.default() if callable(self.default) else self.default)

    def trip_set(self):
        """Indicates that this field is somehow being set."""
        assert not self.immutable, "If %s.hash(field) is called, field is" \
            " marked as immutable"
        self.dirty = True
        self.has_value = True
        if self.instance:
            self.instance.dirty = True

    def reset_state(self):
        """Resets this field's dirty/immutable state."""
        self.immutable = False
        self.dirty = False

    def mark_clean(self):
        """Marks this field as not being dirty."""
        self.dirty = False

    def set(self, value):
        """Set this field to a specific value."""
        self.validate(value)
        self.trip_set()
        self._set(value)

    def validate(self, value):
        """Validate that the input is acceptable.

        Subclasses will probably want to override this.
        """
        if self.instance is not None and self.instance.persisted:
            assert self.modifiable, "Field '%s' is not modifiable" % self.key
        if value is None and not self.null:
            raise ValueError("'None' not allowed for field '%s'" % self.key)

    def _set(self, value):
        """Actually perform the set.

        Subclasses can override this to provide custom set behavior."""
        self.value = value

    def clone(self, clean_break=False):
        """Create a clone of this field.

        Set clean_break to True in order to distance the copy from the instance
        with which the original is associated.
        """
        ret = base_clone(self)
        ret.immutable = False
        if clean_break:
            ret.instance = None
            ret.modifiable = True
        return ret

    # accessor functions

    def __set__(self, instance, value):
        if instance is not None:
            instance.__dict__[self._key].set(value)

    def __get__(self, instance, owner):
        if instance is not None:
            return instance.__dict__[self._key]
        else:
            return self

    def __delete__(self, instance):
        if instance is not None:
            instance.__dict__[self._key].set(None)

    # Functions for delegation

    def __getattr__(self, name):
        return getattr(self.value, name)

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


class Foreign(Field):
    """A field to represent a foreign collection or object.

    This is not very magic.  Once something has been queried, it is set.  The
    object(s) themselves will continue to update, but which object(s) are
    present will not change.
    """

    def __init__(self, foreign_class, local_key, foreign_key, name=None,
                 many=True):
        self.foreign_class = foreign_class
        """The class for object(s) to which this foreign field points."""

        self.foreign_key = foreign_key
        """The key in the foreign class to which this field refers."""

        self.local_key = local_key
        """The key in the local class referring to the foreign object(s)."""

        self.many = many
        """Whether this field refers to a single object or to many objects.

        Many-to-many relationships would require keys in both classes.
        """

        super(Foreign, self).__init__(name=name, modifiable=False)


    # accessor methods

    # These are both for the purposes of faking the obj.value variable
    # as if it were always-already set to the SchemaCollection or
    # Object.

    def __setattribute__(self, name, value):
        # ensure that self.value never actually exists.
        if name == 'value':
            self._value = value
        else:
            super(Foreign, self).__setattribute__(name, value)

    def __getattr__(self, name):
        if name == 'value':
            if self.instance is None:
                return None
            elif self.has_value:
                return self.__dict__['_value']
            else:
                if self.many:
                    # The import dragons will keep you from doing something more
                    # obvious.
                    from . import schema
                    local_key = getattr(instance, self.local_key)
                    if self.name is not None:
                        ret = schema.SchemaCollection(
                            session=instance.session,
                            path=(instance.path() + (self,)),
                            autoset={self.foreign_key : local_key})
                    else:
                        ret = schema.SchemaCollection(
                            session=instance.session,
                            path=self.foreign_class.coll_path,
                            sticky={'eq': [(self.foreign_key,), local_key]},
                            autoset={self.foreign_key : local_key})
                    self.value = ret
                    self.has_value = True
                    return ret
                else:
                    if self.name is not None:
                        ret = instance.session.query(
                            path=(instance.path() + (self,)))
                    else:
                        ret = instance.session.query(
                            path=self.foreign_class.coll_path(),
                            query={'eq': [(self.foreign_key,),
                                          getattr(instance, self.local_key)]
                                   })
                    if ret:
                        self.value = ret[0]
                        self.has_value = True
                        return ret[0]
                    else:
                        self.value = None
                        self.has_value = True
                        return None
        else:
            # name is not 'value'
            return super(Foreign, self).__getattr__(name)

class BooleanField(Field):
    """A field to represent a boolean value."""

    def validate(self, value):
        super(BooleanField, self).validate(value)
        if not isinstance(value, bool):
            raise TypeError("'%s' object is not a bool, but field '%s' requires"
                            " a bool" % (type(value), self.key))

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
                raise ValueError("'%s' does not appear to be an ISO 8601" \
                                     " string, as required by field '%s'" \
                                     % (value, self.key))
        elif not isinstance(value, datetime.datetime) and value is not None:
            raise TypeError("'%s' object is not datetime, unicode, or str, as" \
                                " required by field '%s'" % type(value))


class StringField(Field):
    """A field to represent string data."""

    def __init__(self, encoding='binary', max_length=None, allow_empty=True,
                 validate = lambda x: True, *args, **kwargs):

        self.max_length = max_length
        """The maximum length for this string."""

        self.allow_empty = True
        """Whether or not the empty string is allowed."""

        self.validate_extra = validate
        """An extra callable for validation."""

        self.encoding = encoding
        """The default encoding of this string."""

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
                self.value_decoded = value
                # no real good choice here...
                self.value_encoded = value.encode('utf-8')
            else:
                self.value_encoded = value
                self.value_decoded = None
            super(StringField, self)._set(value)
        else:
            if isinstance(value, unicode):
                self.value_decoded = value
                self.value_encoded = value.encode(self.encoding)
            else:
                self.value_encoded = value
                self.value_decoded = value.decode(self.encoding)
            super(StringField, self)._set(value)

    def validate(self, value):
        super(StringField, self).validate(value)
        if value is None:
            return
        if not isinstance(value, (unicode, str)):
            raise TypeError("'%s' object is neither unicode nor bytes, as" \
                                " required by field '%s'" \
                                % (type(value), self.key))
        if self.max_length is not None \
                and len(value) > self.max_length:
            raise ValueError("'%s' longer than maximum length permitted for" \
                                 " field '%s'" % (value, self.key))
        if not self.allow_empty and value == "":
            raise ValueError("Empty string not permitted for field '%s'" \
                                 % self.key)
        if not self.validate_extra(value):
            raise ValueError("'%s' failed validation for field '%s'" \
                                 % (value, self.key))

    # deal correctly with encodings

    def __unicode__(self):
        return self.value_decoded if self.value_decoded is not None \
            else unicode(self.value)

    def decode(self, encoding=None, *args, **kwargs):
        if encoding == self.encoding and self.value_decoded:
            return self.value_decoded
        else:
            return self.value.decode(encoding, *args, **kwargs)

    def encode(self, encoding=None, *args, **kwargs):
        if encoding == self.encoding:
            return self.value_encoded
        else:
            return self.value_decoded.encode(encoding, *args, **kwargs) \
                if self.value_decoded is not None \
                else self.value.encode(encoding, *args, **kwargs)

    # provide string magic

    def __repr__(self):
        return repr(self.value)
    def __str__(self):
        return str(self.value)
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
        """The precision, in bits, of this integer."""

        self.unsigned = unsigned
        """Whether or not this integer can be unsigned."""

        self.maximum = 0
        """The maximum value for this integer."""

        self.minimum = 0
        """The minimum value for this integer."""

        if precision == 64:
            self.maximum = 18446744073709551615 if unsigned \
                else 9223372036854775807
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
            raise TypeError("'%s' object is not integral, as required by" \
                                " field '%s'" % (type(value), self.key))
        if value > self.maximum or value < self.minimum:
            raise ValueError("'%s' out of range for field '%s'" % (value, self.key))

    def _set(self, value):
        if isinstance(value, int) \
                and (self.precision == 64
                     or (self.precision == 32 and self.unsigned)):
            value = long(value)
        super(IntegerField, self)._set(value)


class RealField(NumericField):
    """Field representing all floating point types."""

    def __init__(self, precision='double', *args, **kwargs):
        if precision not in ['half', 'single', 'double', 'quad']:
            raise ValueError("precision must be one of 'half', 'single'," \
                                 " 'double', or 'quad'")
        self.precision = precision
        super(RealField, self).__init__(*args, **kwargs)

    def validate(self, value):
        super(RealField, self).validate(value)
        if not isinstance(value, float):
            raise TypeError("'%s' object is not float, as required by field" \
                                " '%s'" % (type(value), self.key))


class EnumField(StringField):
    """A field to represent enumerations."""

    def __init__(self, choices, *args, **kwargs):
        self.choices = choices
        """The possible values of this enumeration."""
        super(EnumField, self).__init__(*args, **kwargs)

    def validate(self, value):
        super(EnumField, self).validate(value)
        if value not in self.choices:
            raise ValueError("'%s' not in choices for field '%s': %s" \
                                 % (value, self.key, self.choices.join(", ")))


class UUIDField(StringField):
    """A field to represent UUIDs."""
    validate_re = re.compile(
        '\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\Z')

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
            raise ValueError("'%s' is not a valid UUID, as required by field" \
                                 " '%s'" % (str(value), self.key))

class MultiField(Field):
    """An abstract field representing multiple uniformly-typed values."""

    def _element_to_field(self, element):
        """Transform an element to the underlying field type."""
        f = self.field.clone()
        f.instance = self.instance
        f.set(element.value if isinstance(element, Field) else element)
        return f

    def __init__(self, element_type, *args, **kwargs):
        self.field = element_type
        """The type for all the members of this MultiField."""

        super(MultiField, self).__init__(*args, **kwargs)

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

    # Provide sequence magic

    def __iter__(self):
        return self.value.__iter__()
    def __len__(self):
        return len(self.value)
    def __contains__(self, elem):
        return elem in self.value


class ListField(MultiField):
    """A field to represent a list of other fields of a uniform type."""

    def clone(self, clean_break=False):
        copy = super(ListField, self).clone(clean_break)
        copy.value = [element.clone(clean_break) for element in copy.value]
        return copy

    def _set(self, value):
        newvalue = [self._element_to_field(element) for element in value]
        super(ListField, self)._set(newvalue)

    def __setitem__(self, key, value):
        self.trip_set()
        if isinstance(key, slice):
            self.value[key] = [self._element_to_field(element) \
                                   for element in value]
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
        self.value.extend([self._element_to_field(element) \
                               for element in elems])

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


class SetField(MultiField):
    """A field to represent a set of other fields of a uniform type."""

    def clone(self, clean_break=False):
        copy = super(SetField, self).clone(clean_break)
        copy.value = set([element.clone(clean_break) for element in copy.value])
        return copy

    def _set(self, instance, value):
        newvalue = set([self._element_to_field(element) for element in value])
        super(SetField, self)._set(instance, newvalue)

    def update(self, *others):
        self.trip_set()
        self.value.update([[self._element_to_field(element) \
                                for element in other] \
                               for other in others])
    def __ior__(self, other):
        self.trip_set()
        if not isinstance(other, (set, frozenset)):
            # trigger appropriate error message
            self.value |= other
        self.value |= frozenset([self._element_to_field(element) \
                                     for element in other])
    def intersection_update(self, *others):
        self.trip_set()
        self.value.intersection_update([[self._element_to_field(element) \
                                             for element in other] \
                                            for other in others])
    def __iand__(self, other):
        self.trip_set()
        if not isinstance(other, (set, frozenset)):
            # trigger appropriate error message
            self.value &= other
        self.value &= frozenset([self._element_to_field(element) \
                                     for element in other])
    def difference_update(self, *others):
        self.trip_set()
        self.value.difference_update([[self._element_to_field(element) \
                                           for element in other] \
                                          for other in others])
    def __isub__(self, other):
        self.trip_set()
        if not isinstance(other, (set, frozenset)):
            # trigger appropriate error message
            self.value -= other
        self.value -= frozenset([self._element_to_field(element) \
                                     for element in other])
    def symmetric_difference_update(self, *others):
        self.trip_set()
        self.value.symmetric_difference_update([[self._element_to_field(element) \
                                                     for element in other] \
                                                    for other in others])
    def __ixor__(self, other):
        self.trip_set()
        if not isinstance(other, (set, frozenset)):
            # trigger appropriate error message
            self.value ^= other
        self.value ^= frozenset([self._element_to_field(element) \
                                     for element in other])
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

    def __or__(self, other):
        return self.value | other
    def __and__(self, other):
        return self.value & other
    def __sub__(self, other):
        return self.value - other
    def __xor__(self, other):
        return self.value ^ other
    def copy(self):
        return frozenset() + self.value

    def __hash__(self):
        self.value = frozenset(self.value)
        return super(ListField, self).__hash__(self)
