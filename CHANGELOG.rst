Changelog
-----------
1.5.0
"""""
12 Jan, 2021

* Add support for enum types

1.4.3
"""""
16 Oct, 2020

* Fix error in packaging.

1.4.2
"""""
13 Oct, 2020

* Exceptions raised in value formatter functions augmented
  with column name and value.

1.4.1
"""""
28 Jan, 2020

* Support for ``time`` datatype (thanks Nathan Glover)

1.4.0
"""""
11 Jul, 2019

* Transparent string encoding
* Support for array types
* Schema-aware :class:`pgcopy.Replace`
* Search-path-aware default schema
* Expanded test suite, using pytest
* Expanded documentation
* Travis tests on python 3.7 and PostgreSQL 10

1.3.1
"""""
14 Feb, 2018

* Mention ``commit`` in the README

1.3.0
"""""
22 Aug, 2017

* Support unlimited varchar fields (thanks John A. Bachman)
* Updated documentation regarding string encoding in Python 3 (thanks John
  A. Bachman)
* Fix bug in varchar truncation
* Fix bug in numeric type formatter (reported by Peter Van Eynde)

1.2.0
"""""
25 Mar, 2017

* Support db schema (thanks Marcin Gozdalik)

1.1.0
"""""
26 Jan, 2017

* Support ``uuid``, ``json``, and ``jsonb`` types
  (thanks Igor Mastak)
* Integrate Travis CI
* Add docker test strategy

1.0.0
"""""
19 Jan, 2017

* Run tests with tox
* Support Python 3
* Initial release on PyPi

0.7
"""
19 Jan, 2017

* Add support for serializing Python ``decimal.Decimal`` to PostgreSQL ``numeric``.

0.6
"""
21 Oct, 2014

* :class:`pgcopy.util.RenameReplace` variant

0.5
"""
14 Jul, 2014

* Support default values and sequences

0.4
"""
14 Jul, 2014

* Fix :class:`pgcopy.Replace` utility class bugs
* Add view support to :class:`pgcopy.Replace`

0.3
"""
8 Jul, 2014

*  Move Cython optimization to separate project
*  Add :class:`pgcopy.Replace` utility class

0.2
"""
7 Jul, 2014

*  Cython optimization

0.1
"""
29 Jun, 2014

*  Initial version
