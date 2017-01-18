pgcopy
=================

pgcopy is a small system for very fast bulk insertion of data into a
PostgreSQL database table using `binary copy`_.

Installation
-------------

pgcopy requires pytz_ and the psycopg2_ db adapter.
nose_ is required to run the tests.

Use
---------

pgcopy provides facility for copying data from an iterable of tuple-like
objects using a ``CopyManager``, which must be instantiated with a psycopg2
db connection, the table name, and an iterable indicating the names of the
columns to be inserted in the order in which they will be provided.
pgcopy inspects the database to determine the datatypes of the columns.

For example::

    from datetime import datetime
    from pgcopy import CopyManager
    import psycopg2
    cols = ('id', 'timestamp', 'location', 'temperature')
    now = datetime.now()
    records = [
            (0, now, 'Jerusalem', 72.2),
            (1, now, 'New York', 75.6),
            (2, now, 'Moscow', 54.3),
        ]
    conn = psycopg2.connect(database='weather_db')
    mgr = CopyManager(conn, 'measurements_table', cols)
    mgr.copy(records)

By default, a temporary file on disk is used.  If there's enough memory,
you can get a slight performance benefit with in-memory storage::

    from cStringIO import StringIO
    mgr.copy(records, StringIO)

Supported datatypes
-------------------

Currently the following PostgreSQL datatypes are supported:

* bool
* smallint
* integer
* bigint
* real
* double precision
* char
* varchar
* text
* bytea
* date
* timestamp
* timestamp with time zone
* numeric (data must be ``decimal.Decimal``)

.. note::

    PostgreSQL numeric does not support ``Decimal('Inf')`` or
    ``Decimal('-Inf')``.  pgcopy serializes these as ``NaN``.

Benchmarks
-----------

Below are simple benchmarks for 100000 records.
This gives a general idea of the kind of speedup 
available with pgcopy::

    $ nosetests -c tests/benchmark.cfg 
              ExecuteManyBenchmark:   7.75s
                   PGCopyBenchmark:   0.54s
    ----------------------------------------------------------------------
    Ran 2 tests in 9.101s

Replacing a Table
------------------

When possible, faster insertion may be realized by inserting into an empty
table with no indices or constraints.  In a case where the entire contents
of the table can be reinserted, the ``Replace`` context manager automates
the process.  On entry, it creates a new table like the original, with a
temporary name.  Default column values are included.  It provides the
temporary name for populating the table within the context.  On exit, it
recreates the constraints, indices, triggers, and views on the new table,
then replaces the old table with the new.  It can be used so::

    from pgcopy import CopyManager, Replace
    with Replace(conn, 'mytable') as temp_name:
        mgr = CopyManager(conn, temp_name, cols)
        mgr.copy(records)

``Replace`` renames new db objects like the old, where possible.
Names of foreign key and check constraints will be mangled.
As of v0.6 there is also ``pgcopy.util.RenameReplace``, which instead of
dropping the original objects renames them using a transformation function.

Note that on PostgreSQL 9.1 and earlier, concurrent queries on the table
`will fail`_ once the table is dropped.

.. _will fail: https://gist.github.com/altaurog/ab0019837719d2a93e6b

See Also
--------

cpgcopy_, a Cython implementation, about twice as fast.


.. _binary copy: http://www.postgresql.org/docs/9.3/static/sql-copy.html
.. _psycopg2: https://pypi.python.org/pypi/psycopg2/
.. _pytz: https://pypi.python.org/pypi/pytz/
.. _nose: https://pypi.python.org/pypi/nose/
.. _cpgcopy: https://github.com/altaurog/cpgcopy
