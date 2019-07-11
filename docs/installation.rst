Installation
-----------------

To install::

    $ pip install pgcopy

Dependencies
""""""""""""
pgcopy requires pytz_ and the psycopg2_ db adapter.
pytest_ is required to run the tests.

Due to technical problems with the psycopg2 binary distributions, `versions
2.8 and later`_ have separate packages for binary install.  This complicates
installation in some situations, as it requires the dev tools to build psycopg2.

If you do not want to build psycopg2 for each installation, the recommended
options are:

* install psycopg2 version 2.7.X before installing pgcopy
* create a psycopg2 wheel for distribution to production machines

Compatibility
"""""""""""""
pgcopy is tested with Python versions 2.7, 3.4 -- 3.7 and
PostgreSQL versions 9.1 -- 11

.. _psycopg2: https://pypi.org/project/psycopg2/
.. _pytz: https://pypi.org/project/pytz/
.. _pytest: https://pypi.org/project/pytest/
.. _versions 2.8 and later: http://initd.org/psycopg/docs/news.html#what-s-new-in-psycopg-2-8
