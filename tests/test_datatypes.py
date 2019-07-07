# -*- coding: utf-8 -*-
from __future__ import unicode_literals
import json
import decimal
import sys
import uuid
import pytest

if sys.version_info < (3,):
    memoryview = buffer

import psycopg2.extensions
from pgcopy import CopyManager, util

from . import db

def test_connection_encoding(conn):
    assert conn.encoding == 'UTF8'

def test_db_encoding(conn):
    assert conn.info.parameter_status('server_encoding') == 'UTF8'

class TypeMixin(db.TemporaryTable):
    null = 'NOT NULL'
    record_count = 3
    def test_type(self, conn, cursor, schema_table, data):
        bincopy = CopyManager(conn, schema_table, self.cols)
        bincopy.copy(data)
        select_list = ','.join(self.cols)
        cursor.execute("SELECT %s from %s" % (select_list, schema_table))
        self.checkResults(cursor, data)

    def checkResults(self, cursor, data):
        for rec in data:
            self.checkValue(rec, cursor.fetchone())

    def checkValue(self, expected, found):
        for a, b in zip(self.expected(expected), found):
            assert a == self.cast(b)

    def cast(self, v): return v

    expected = cast

class TestEncoding(TypeMixin):
    tempschema = False
    datatypes = ['varchar(12)']
    data = [('database',), ('מוסד נתונים',)]

    @pytest.mark.parametrize('conn', ['UTF8', 'ISO_8859_8', 'WIN1255'], indirect=True)
    def test_type(self, conn, cursor, schema_table, data):
        psycopg2.extensions.register_type(psycopg2.extensions.UNICODE, cursor)
        super(TestEncoding, self).test_type(conn, cursor, schema_table, data)

class TestInteger(TypeMixin):
    datatypes = ['integer']

class TestBool(TypeMixin):
    datatypes = ['bool']

class TestSmallInt(TypeMixin):
    datatypes = ['smallint']

class TestBigInt(TypeMixin):
    datatypes = ['bigint']

class TestReal(TypeMixin):
    datatypes = ['real']

class TestDouble(TypeMixin):
    datatypes = ['double precision']

class TestNull(TypeMixin):
    null = 'NULL'
    datatypes = ['integer']
    data = [(1,), (2,), (None,)]

class TestVarchar(TypeMixin):
    datatypes = ['varchar(12)', 'varchar']

    data = [
        (b'', b'',),
        (b'one', b'one',),
        (b'one two four', b'one two four',),
        (b'one two three', b'one two three',),
    ]

    def cast(self, v):
        return v.strip().encode()

    def expected(self, v):
        return (v[0][:12], v[1])


class TestChar(TypeMixin):
    datatypes = ['char(12)']

    data = [
        (b'',),
        (b'one',),
        (b'one two four',),
        (b'one two three',),
    ]

    def cast(self, v):
        return v.encode()

    def expected(self, v):
        return (v[0][:12].ljust(12),)

class TestText(TypeMixin):
    datatypes = ['text']
    data = [
        (b'Fourscore and seven years ago our fathers set forth '
         b'on this continent a new nation',),
        (b'Python is a programming language that lets you work quickly '
         b'and integrate systems more effectively.',),
    ]

    def cast(self, v):
        return v.encode()


class TestJSON(TypeMixin):
    datatypes = ['json']
    data = [
        (b'{"data": "Fourscore and seven years ago our fathers set forth"}',),
        (b'{"data": "Python is a programming language that lets you work quickly"}',),
    ]

    def cast(self, v):
        return json.dumps(v).encode()


class TestJSONB(TypeMixin):
    datatypes = ['jsonb']
    data = [
        (b'{"data": "Fourscore and seven years ago our fathers set forth"}',),
        (b'{"data": "Python is a programming language that lets you work quickly"}',),
    ]

    def cast(self, v):
        return json.dumps(v).encode()


class TestBytea(TypeMixin):
    datatypes = ['bytea']
    data = [
        (b'\x02\xf3\x18\x44 alphabet',),
        (b'The\x00hippopotamus\x00jumped\x00over\x00the\x00lazy\x00tower',)
    ,]

    def cast(self, v):
        assert isinstance(v, memoryview)
        return bytes(v)

class TestTimestamp(TypeMixin):
    datatypes = ['timestamp']

class TestTimestampTZ(TypeMixin):
    datatypes = ['timestamp with time zone']

    def cast(self, v):
        return util.to_utc(v)

class TestDate(TypeMixin):
    datatypes = ['date']

class TestNumeric(TypeMixin):
    datatypes = ['numeric']

    data = [
        (decimal.Decimal('100'),),
        (decimal.Decimal('10000'),),
        (decimal.Decimal('-1000'),),
        (decimal.Decimal('21034.56'),),
        (decimal.Decimal('-900000.0001'),),
        (decimal.Decimal('-1.3E25'),),
    ]

class TestNumericNan(TypeMixin):
    datatypes = ['numeric']
    data = [(decimal.Decimal('NaN'),),]

    def checkResults(self, cursor, data):
        assert cursor.fetchone()[0].is_nan()


class TestUUID(TypeMixin):
    datatypes = ['uuid']
    data = [
        (uuid.UUID('55daa192-a28a-4c49-ae84-ef3564e32308'),),
        (uuid.UUID('8b56420e-7e15-4bae-a76e-af20e35ea88f'),),
    ]

    def cast(self, v):
        return uuid.UUID(v)
