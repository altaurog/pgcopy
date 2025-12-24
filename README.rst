.. home-start

pgcopy
=======

.. image:: https://github.com/altaurog/pgcopy/actions/workflows/test.yaml/badge.svg?branch=master
    :target: https://github.com/altaurog/pgcopy/actions/workflows/test.yaml?query=branch%3Avector

.. image:: https://coveralls.io/repos/github/altaurog/pgcopy/badge.svg?branch=master
    :target: https://coveralls.io/github/altaurog/pgcopy?branch=master

.. image:: https://img.shields.io/pypi/l/pgcopy.svg
    :target: https://pypi.org/project/pgcopy/

.. image:: https://img.shields.io/pypi/wheel/pgcopy.svg
    :target: https://pypi.org/project/pgcopy/

.. image:: https://img.shields.io/pypi/pyversions/pgcopy.svg
    :target: https://pypi.org/project/pgcopy/

Use pgcopy_ for fast data loading into
PostgreSQL with `binary copy`_.

.. _binary copy: http://www.postgresql.org/docs/9.3/static/sql-copy.html
.. _pgcopy: https://pgcopy.readthedocs.io/en/latest/

Features
---------
* Support for multiple db adaptors
* Support for many data types
* Support for multi-dimensional array types
* Support for schema and schema search path
* Support for mixed-case table and column names
* Transparent string encoding
* Utility for replacing entire table

Quickstart
-----------

.. quickstart-start

::

    from datetime import datetime
    from pgcopy import CopyManager
    import psycopg
    cols = ('id', 'timestamp', 'location', 'temperature')
    now = datetime.now()
    records = [
            (0, now, 'Jerusalem', 72.2),
            (1, now, 'New York', 75.6),
            (2, now, 'Moscow', 54.3),
        ]
    conn = psycopg.connect(database='weather_db')
    mgr = CopyManager(conn, 'measurements_table', cols)
    mgr.copy(records)

    # don't forget to commit!
    conn.commit()

.. home-end

Supported Adaptors
-------------------

* psycopg2_
* psycopg_
* pg8000_
* PyGreSQL_

Supported datatypes
-------------------

pgcopy supports the following PostgreSQL scalar types:

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
* enum types
* date
* time
* timestamp
* timestamp with time zone
* numeric
* json
* jsonb
* uuid
* arrays
* vector

Documentation
--------------

`Read the docs.`_

.. _Read the docs.: pgcopy_


.. _binary copy: http://www.postgresql.org/docs/9.3/static/sql-copy.html
.. _psycopg2: https://pypi.org/project/psycopg2/
.. _psycopg: https://pypi.org/project/psycopg/
.. _pg8000: https://pypi.org/project/pg8000/
.. _PyGreSQL: https://pypi.org/project/PyGreSQL/

.. _pytz: https://pypi.org/project/pytz/
.. _pytest: https://pypi.org/project/pytest/
.. _Tox: https://tox.readthedocs.io/en/latest/
