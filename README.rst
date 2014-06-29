pgcopy
=================

pgcopy is a small system for very fast bulk insertion of data into a
PostgreSQL database table using `binary copy`_.

Installation
-------------

The easy way::

    pip install pgcopy

pgcopy requires the psycopg2_ db adapter.  nose_ is required to run the tests.

Basic use
---------

pgcopy provides facility for copying data from an iterable of tuple-like
objects using a `CopyManager`, which must be instantiated with a psycopg2
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
            (0, now, 'New York', 75.6),
            (0, now, 'Moscow', 54.3),
        ]
    conn = psycopg2.connect(database='weather')
    mgr = CopyManager(conn, 'measurements', cols)
    mgr.copy(records)

Supported datatypes
-------------------

Currently the following PostgreSQL datatypes are supported:

* integer
* bool
* smallint
* bigint
* real
* double precision
* integer
* varchar
* char
* text
* bytea
* timestamp
* timestamp with time zone
* date

.. _binary copy: http://www.postgresql.org/docs/9.3/static/sql-copy.html
.. _psycopg2: http://initd.org/psycopg/
.. _nose: http://nose.readthedocs.org/
