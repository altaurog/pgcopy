Installation
-----------------

To install::

    $ pip install pgcopy

Dependencies
""""""""""""
pgcopy requires Python3, pytz_, and a db adaptor.  The supported adaptors are:

* psycopg2_
* psycopg_
* pg8000_
* PyGreSQL_

pytest_ and one of psycopg2_ or psycopg_ is required to run the tests.

Compatibility
"""""""""""""
pgcopy is tested with Python versions 3.9 -- 3.14 and
PostgreSQL versions 13 -- 18, as well as `Aurora DSQL`_
(note DSQL does not support all pgcopy features).

.. note::

    Python 2.7 is no longer supported!
    Please upgrade to Python 3.

.. _psycopg2: https://pypi.org/project/psycopg2/
.. _psycopg: https://pypi.org/project/psycopg/
.. _pg8000: https://pypi.org/project/pg8000/
.. _PyGreSQL: https://pypi.org/project/PyGreSQL/
.. _pytz: https://pypi.org/project/pytz/
.. _pytest: https://pypi.org/project/pytest/
.. _Aurora DSQL: https://docs.aws.amazon.com/aurora-dsql/latest/userguide/what-is-aurora-dsql.html
