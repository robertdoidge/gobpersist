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

    def __init__(self, null=False, unique=False, primary_key=False,
                 name=None, default=None, default_update=None,
                 revision_tag=False, modifiable=True):
        self.null = null
        """Whether or not the field may be None."""

        self.unique = unique
        """Indicates that this field must be unique."""

        self.default = default
        """Callable or scalar indicating the default value for this field."""

        self.default_update = default_update
        """Callable indicating a transformation to perform on
        unaltered values for update.

        The callable will be called like this:
        value = default_update(value)
        """

        self.revision_tag = revision_tag
        """Indicates whether this field should be considered a
        revision tag or not.

        A revision tag must be unchanged between reading the object and
        updating/removing the object.
        """

        self.primary_key = primary_key
        """Indicates whether or not this field is the primary key for this
        object."""

        self.modifiable = modifiable
        """Indicates whether or not this field can be modified for update."""

        self.name = name
        """Hints at the name of this field, if different from the variable name
        that refers to it."""

        self.instance_key = None
        """The key used to store data on the instance."""

        self.dirty = True
        """Whether or not this field has been altered."""

        self.immutable = False
        """Indicates that this field has become immutable.
        
        This will be set True when the hash() function is called on this object,
        whether explicitly or through the use of this field as a dictionary key
        or a member of a set.
        """

        self._name = None
        """The name of the variable that refers to this field."""

        self.value = None
        """The actual value of this field."""

        self.has_value = False
        """Whether or not a value has been set.

        This can be used to distinguish between the absence of some
        datum within gobpersist and the absence of some datum within
        the data store.
        """

        self.instance = None
        """The Gob instance for this instance of Field.

        None, if this represents an abstract field or a field on a
        class.
        """

        self.persisted_value = None
        """The value of the field as it is stored in the database."""

        self.has_persisted_value = False
        """Whether or not a persisted value has been set."""


    def prepare_add(self):
        """Prepares this field to be added to the store."""
        if not self.has_value:
            self.set(self.default() if callable(self.default) else self.default)

    def prepare_update(self):
        """Prepares this field to be updated in the store."""
        if not self.dirty and self.default_update is not None:
            self.set(self.default_update(self.value))

    def trip_set(self):
        """Indicates that this field is somehow being set."""
        assert not self.immutable, "If %s.hash() is called, field is" \
            " marked as immutable" % self._name
        self.dirty = True
        self.has_value = True
        if self.instance:
            self.instance.dirty = True

    def reset_state(self):
        """Resets this field's dirty/immutable state."""
        self.immutable = False
        self.dirty = False

    def mark_persisted(self):
        """Marks this field as persisted in the database."""
        self.dirty = False
        if self.has_value:
            self.persisted_value = self.value
            self.has_persisted_value = True

    def revert(self):
        """Reverts this field to the persisted version."""
        assert not self.immutable, "If %s.hash() is called, field is" \
            " marked as immutable" % self._name
        self.dirty = False
        self.value = self.persisted_value
        self.has_value = self.has_persisted_value

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
            assert self.modifiable, "Field '%s' is not modifiable" % self._name
        if value is None and not self.null:
            raise ValueError("'None' not allowed for field '%s'" % self._name)

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
            instance.__dict__[self.instance_key].set(value)

    def __get__(self, instance, owner):
        if instance is not None:
            return instance.__dict__[self.instance_key]
        else:
            return self

    def __delete__(self, instance):
        if instance is not None:
            instance.__dict__[self.instance_key].set(None)

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
        hash_ = hash(self.value)
        self.immutable = True
        return hash_
    def __nonzero__(self):
        return bool(self.value)
        

class BooleanField(Field):
    """A field to represent a boolean value."""

    def validate(self, value):
        super(BooleanField, self).validate(value)
        if value is None:
            return
        if not isinstance(value, bool):
            raise TypeError("'%s' object is not a bool, but field '%s' requires"
                            " a bool" % (type(value), self._name))


class DateTimeField(Field):
    """A field to represent a point in time."""

    def _set(self, value):
        if isinstance(value, (unicode, str)):
            value = iso8601.parse_datetime(value)
        super(DateTimeField, self)._set(value)

    def validate(self, value):
        super(DateTimeField, self).validate(value)
        if value is None:
            return
        if isinstance(value, (unicode, str)):
            if iso8601.ISO8601_RE.match(value) is None:
                raise ValueError("'%s' does not appear to be an ISO 8601" \
                                     " string, as required by field '%s'" \
                                     % (value, self._name))
        elif not isinstance(value, datetime.datetime) and value is not None:
            raise TypeError("'%s' object is not datetime, unicode, or str, as" \
                                " required by field '%s'" % type(value))


class TimestampField(DateTimeField):
    """A convenience wrapper for datetime that by default sets the
    value to the latest time it was persisted."""
    def __init__(self, *args, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = lambda: datetime.datetime.utcnow()
        if 'default_update' not in kwargs:
            kwargs['default_update'] = lambda value: datetime.datetime.utcnow()

        super(TimestampField, self).__init__(*args, **kwargs)


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
                                % (type(value), self._name))
        if self.max_length is not None \
                and len(value) > self.max_length:
            raise ValueError("'%s' longer than maximum length permitted for" \
                                 " field '%s'" % (value, self._name))
        if not self.allow_empty and value == "":
            raise ValueError("Empty string not permitted for field '%s'" \
                                 % self._name)
        if not self.validate_extra(value):
            raise ValueError("'%s' failed validation for field '%s'" \
                                 % (value, self._name))

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
        if value is None:
            return
        if not isinstance(value, (int,long)):
            raise TypeError("'%s' object is not integral, as required by" \
                                " field '%s'" % (type(value), self._name))
        if value > self.maximum or value < self.minimum:
            raise ValueError("'%s' out of range for field '%s'" \
                                 % (value, self._name))

    def _set(self, value):
        if isinstance(value, int) \
                and (self.precision == 64
                     or (self.precision == 32 and self.unsigned)):
            value = long(value)
        super(IntegerField, self)._set(value)


class IncrementingField(IntegerField):
    """A convenience wrapper for IntegerField that by default
    increments the value by one every time it is persisted."""
    def __init__(self, *args, **kwargs):
        if 'default' not in kwargs:
            kwargs['default'] = 0
        if 'default_update' not in kwargs:
            kwargs['default_update'] = lambda value: value + 1
        super(IncrementingField, self).__init__(*args, **kwargs)


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
        if value is None:
            return
        if not isinstance(value, float):
            raise TypeError("'%s' object is not float, as required by field" \
                                " '%s'" % (type(value), self._name))


class EnumField(StringField):
    """A field to represent enumerations."""

    def __init__(self, choices, *args, **kwargs):
        self.choices = choices
        """The possible values of this enumeration."""
        super(EnumField, self).__init__(*args, **kwargs)

    def validate(self, value):
        super(EnumField, self).validate(value)
        if value is None:
            return
        if value not in self.choices:
            raise ValueError("'%s' not in choices for field '%s': %s" \
                                 % (value, self._name, self.choices.join(", ")))


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
                                 " '%s'" % (str(value), self._name))


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
    def prepare_update(self):
        super(MultiField, self).prepare_update()
        for element in self.value:
            element.prepare_update()
    def prepare_add(self):
        super(MultiField, self).prepare_add()
        for element in self.value:
            element.prepare_add()

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
        self.value.symmetric_difference_update(
            [[self._element_to_field(element) \
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


class Foreign(Field):
    """An abstract field to represent a foreign collection or object.

    This is not very magic.  Once something has been queried, it is
    set.  The object(s) themselves will continue to update, but which
    object(s) are present will not change.  If you want to explicitly
    reload the field, use field.forget() to forget the cached value.
    """

    def __init__(self, foreign_class, local_field, foreign_field, key=None,
                 name=None):
        self.foreign_class = foreign_class
        """The class for object(s) to which this foreign field points."""

        self.foreign_field = foreign_field
        """The key in the foreign class to which this field refers."""

        self.local_field = local_field
        """The key in the local class referring to the foreign object(s)."""

        self.key = key
        """A key for this relationship.

        If key is None (the default), then this foreign relationship
        is virtual and will be constructed through a query.
        """

        super(Foreign, self).__init__(name=name, modifiable=False)


    def mark_persisted(self):
        pass

    def revert(self):
        """For a foreign field, revert() will drop the cached value."""
        self.forget()

    def forget(self):
        """Forget the cached value."""
        self.has_value = False
        self._value = None

    def prepare_add(self):
        pass

    def prepare_update(self):
        pass

    # accessor methods

    # Both of these are for the purposes of faking the obj.value variable
    # as if it were always-already set to the SchemaCollection or
    # Object.

    def __setattribute__(self, name, value):
        # ensure that self.value never actually exists.
        if name == 'value':
            self._value = value
        else:
            super(Foreign, self).__setattribute__(name, value)

    def fetch_value(self):
        """Fetches the appropriate foreign value on demand."""
        pass

    def __getattr__(self, name):
        if name == 'value':
            if self.instance is None:
                return None
            elif self.has_value:
                return self._value
            else:
                self._value = self.fetch_value()
                self.has_value = True
                return self._value
        else:
            return super(Foreign, self).__getattr__(name)

    # Functions for delegation

    def __repr__(self):
        return self.__class__.__name__ + '(' + self._name + ')'
    def __str__(self):
        return self._name
    def __lt__(self, other):
        return self.__class__ < other.__class__ \
                if self.__class__ is not other.__class__ \
            else self.foreign_class < other.foreign_class \
                if self.foreign_class is not other.foreign_class \
            else self.name < other.name
    def __le__(self, other):
        return self.__class__ <= other.__class__ \
                if self.__class__ is not other.__class__ \
            else self.foreign_class <= other.foreign_class \
                if self.foreign_class is not other.foreign_class \
            else self.name <= other.name
    def __eq__(self, other):
        return False if self.__class__ is not other.__class__ \
            else False if self.foreign_class is not other.foreign_class \
            else self.name == other.name
    def __ne__(self, other):
        return True if self.__class__ is not other.__class__ \
            else True if self.foreign_class is not other.foreign_class \
            else self.name != other.name
    def __gt__(self, other):
        return self.__class__ > other.__class__ \
                if self.__class__ is not other.__class__ \
            else self.foreign_class > other.foreign_class \
                if self.foreign_class is not other.foreign_class \
            else self.name > other.name
    def __ge__(self, other):
        return self.__class__ >= other.__class__ \
                if self.__class__ is not other.__class__ \
            else self.foreign_class >= other.foreign_class \
                if self.foreign_class is not other.foreign_class \
            else self.name >= other.name
    def __cmp__(self, other):
        return cmp(self.__class__, other.__class__) \
                if self.__class__ is not other.__class__ \
            else cmp(self.foreign_class, other.foreign_class) \
                if self.foreign_class is not other.foreign_class \
            else cmp(self.name, other.name)
    def __hash__(self):
        return hash(hash(self.__class__)
                    + hash(self.foreign_class)
                    + hash(self.name))
    def __nonzero__(self):
        return False if self.has_value and self._value is None else True


class ForeignObject(Foreign):
    """A field to represent a foreign object."""

    def fetch_value(self):
        if self.key is None:
            ret = instance.session.query(
                cls=self.foreign_class,
                query={'eq': [(self.foreign_field,),
                              getattr(instance, self.local_field)]
                       })
        else:
            ret = instance.session.query(
                cls=self.foreign_class,
                key=self.key)
        if ret:
            return ret[0]
        else:
            return None


class ForeignCollection(Foreign):
    """A field representing a foreign collection."""

    def fetch_value(self):
        # The import dragons will keep you from doing something more
        # obvious.
        from . import schema
        local_field = getattr(self.instance, self.local_field)
        if self.key is None:
            return schema.SchemaCollection(
                cls=self.foreign_class,
                session=instance.session,
                sticky={'eq': [(self.foreign_field,), local_field]},
                autoset={self.foreign_field : local_field})
        else:
            return schema.SchemaCollection(
                cls=self.foreign_class,
                session=instance.session,
                key=self.key,
                autoset={self.foreign_field : local_field})
