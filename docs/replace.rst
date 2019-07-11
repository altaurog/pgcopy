Replace
------------------

When possible, faster insertion may be realized by `inserting into an empty
table`_ with no indices or constraints.  In a case where the entire contents
of the table can be reinserted, the :class:`pgcopy.Replace` context manager automates
the process.

.. autoclass:: pgcopy.Replace

As of v0.6 there is also ``pgcopy.util.RenameReplace``, which instead of
dropping the original objects renames them using a transformation function.

.. autoclass:: pgcopy.util.RenameReplace

.. _inserting into an empty table: http://dba.stackexchange.com/a/41111/9941
