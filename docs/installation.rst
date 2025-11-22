Installation
-----------------

To install::

    $ pip install pgcopy

Dependencies
""""""""""""
pgcopy requires pytz_ and the psycopg2_ db adapter.
pytest_ is required to run the tests.

Due to technical problems with binary distributions, `psycopg2 versions
2.8 and later`_ have separate packages for binary install.  This complicates
installation in some situations, as it requires the dev tools to build psycopg2.

If you do not want to build psycopg2 for each installation, the recommended
approach is to create a psycopg2 wheel for distribution to production machines

Compatibility
"""""""""""""
pgcopy is tested with Python versions 3.8 -- 3.14 and
PostgreSQL versions 12 -- 18

.. note::

    Python 2.7 is no longer supported!
    Please upgrade to Python 3.

.. _psycopg2: https://pypi.org/project/psycopg2/
.. _pytz: https://pypi.org/project/pytz/
.. _pytest: https://pypi.org/project/pytest/
.. _psycopg2 versions 2.8 and later: https://www.psycopg.org/docs/install#change-in-binary-packages-between-psycopg-2-7-and-2-8
