Data Types
-----------

pgcopy supports a wide range of PostgreSQL scalar data types.

Database ``null`` must be represented in python as ``None``.

Scalars
"""""""

========================== ================= =========================
PostgreSQL type            Python type       Notes
========================== ================= =========================
bool                       bool
smallint                   int
integer                    int
bigint                     int
real                       float
double precision           float
char                       str, bytes        Encoding_, Truncation_
varchar                    str, bytes        Encoding_, Truncation_
text                       str, bytes        Encoding_, Truncation_
bytea                      bytes             Truncation_
enum types                 str, bytes        Encoding_
date                       datetime.date
time                       datetime.time
timestamp                  datetime.datetime
timestamp with time zone   datetime.datetime
numeric                    decimal.Decimal   Numeric_
json                       str, bytes        Encoding_
jsonb                      bytes
uuid                       uuid.UUID
vector_                    list[float]       Contrib_
========================== ================= =========================

Arrays
"""""""
As of v1.4.0, all of the supported scalar types may be used in array types as well.

Encoding
"""""""""""
As of v1.4, encoding of unicode strings is handled automatically for ``char``,
``varchar``, ``text``, and ``json`` PostgreSQL types.  Python ``bytes`` may also be
used, provided the encoding matches that of the db connection.

No encoding is performed for data to be inserted into ``bytea`` or
``jsonb`` types.

Truncation
"""""""""""
Where database columns have a fixed length, string data will be silently truncated to fit.

Numeric
""""""""
PostgreSQL numeric does not support ``Decimal('Inf')`` or
``Decimal('-Inf')``.  pgcopy serializes these as ``NaN``.

Contrib
""""""""
Support for ``vector`` type from pgvector_ is available in
``contrib.vector.CopyManager``.

.. _vector: https://github.com/pgvector/pgvector
.. _pgvector: https://github.com/pgvector/pgvector
