.. _query:

Querying Gobpersist
===================

All gobpersist queries consist of a key or key range query, optionally
followed by a query proper.  Support for querying on a range of keys
is currently pretty sketchy, so this document will not focus on it,
for now.  It should be a pretty straightforward extension of
single-key queries, however.

A query consists of a list of Boolean operations, all of which must be
true for the query to succeed.  The arguments to each operator are
either data paths (tuples), quantified paths (dictionaries), literal
values, or, in the case of "and", "or", "nor", or "not", subqueries.

Operators
---------

Gobpersist supports the following operators:

* ``'eq'``: true if all of its arguments are equal to each other.

* ``'ne'``: true if none of its arguments are not equal to each other.

* ``'lt'``: true if each argument is less than the subsequent
  argument.

* ``'gt'``: true if each argument is greater than the subsequent
  argument.

* ``'ge'``: true if each argument is greater than or equal to the the
  subsequent argument.

* ``'le'``: true if each argument is less than or equal to the the
  subsequent argument.

* ``'and'``: true if all of its arguments (subqueries) are true.

* ``'or'``: true if any of its arguments (subqueries) are true.

* ``'nor'``: true if all of its arguments (subqueries) are false.

Though all operators take an arbitrary number of arguments, the
semantics are such that one can always conceive of them as a series of
binary operators joined together with "and", that is::

   {'op': [arg1, arg2, arg3, arg4]}

is always identical to::

   {
       'and': [
           {'op': [arg1, arg2]},
           {'op': [arg2, arg3]},
           {'op': [arg3, arg4]}
       ]
   }

When an operator has only one argument, it is always true for "eq,"
"ne," "lt," "gt," "ge," or "le"; always the value of the subquery for
"and" or "or"; and always the negation of the value of the subquery
for "nor".  For this latter use, "not" is an alias for "nor" to better
express the intention.

Paths
-----

All tuples are interpreted as paths to variables, either scalars or,
in the case of quantified values, collections, lists, or sets.  Most
of the time, a path will consist of a single element, which is the
name of the field whose value you want to compare.  For example,
``('price')`` indicates the value of the :attr:`price` field on the
gob being examined.  If the gob has a :class:`ForeignObject
<gobpersist.field.ForeignObject>` defined on it, you might indicate a
value within the foreign object, similar to a SQL join, by use of a
two valued tuple.  For example ``('favorite_item', 'price')`` gives
the :attr:`price` field on the gob linked up through the
:attr:`favorite_item` foreign object.

Quantifiers
-----------

Supposing, however, that one wants to access the value of something in
a one-to-many relation with the gob in question, one must use a
quantifier.

Gobpersist supports the following quantifiers:

* ``'any'``: true if the operation is true for any field referenced by
  the path

* ``'all'``: true if the operation is true for every field referenced
  by the path

* ``'none'``: true if the operation is false for every field
  referenced by the path

Quantifiers iterate over each one of their objects, testing each one
according to the same semantics as would be used for scalar values.
For example, the following query::

   {'eq': [{'any': ('users_favorited', 'email')}, "test@test.com"]}

is roughly equivalent to the psuedo-query::

   {
       'or': [
           {'eq': [('users_favorited1', 'email'), "test@test.com"]},
           {'eq': [('users_favorited2', 'email'), "test@test.com"]},
           ...
           {'eq': [('users_favoritedN', 'email'), "test@test.com"]}
       ]
   }

Putting it all together
-----------------------

Here is an example query, expressing the full capabilities of the
language::

   {
       'gt': [('price'), 14.35],
       'ne': [{'any': ('users_favorited', 'email')}, None],
       'nor': [
           {'eq': [{'any': ('users_favorited', 'email')}, "test@test.com"]},
           {'eq': [{'any': ('users_favorited', 'email')}, "test@example.com"]}
       ]
   }

This query should retrieve every gob with a price above 14.35 that has
been favorited by any user with an email that is not None, and which
is also neither "test@test.com" nor "test@example.com".
