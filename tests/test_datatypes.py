import itertools
from datetime import date, datetime
from unittest import TestCase
from pgcopy import CopyManager, util
from . import db

class TypeMixin(db.TemporaryTable):
    null = 'NOT NULL'
    def test_type(self):
        self.col = db.numname(0)
        bincopy = CopyManager(self.conn, self.table, [self.col])
        bincopy.copy(self.data)
        self.cur.execute("SELECT %s from %s" % (self.col, self.table))
        for rec in self.data:
            self.checkValues(rec, self.cur.fetchone())

    def checkValues(self, expected, found):
        for a, b in itertools.izip(expected, found):
            self.assertEqual(a, self.cast(b))

    def cast(self, v): return v

class IntegerTest(TypeMixin, TestCase):
    data = [(1,), (2,)]
    datatypes = ['integer']

class BoolTest(TypeMixin, TestCase):
    data = [(True,), (False,)]
    datatypes = ['bool']

class SmallIntTest(TypeMixin, TestCase):
    data = [(1,), (2,)]
    datatypes = ['smallint']

class BigIntTest(TypeMixin, TestCase):
    data = [(1,), (2,)]
    datatypes = ['bigint']

class RealTest(TypeMixin, TestCase):
    data = [(1.5,), (2.25,)]
    datatypes = ['real']

class DoubleTest(TypeMixin, TestCase):
    data = [(1.5,), (2.25,)]
    datatypes = ['double precision']

class NullTest(TypeMixin, TestCase):
    null = 'NULL'
    data = [(1,), (2,), (None,)]
    datatypes = ['integer']

class VarcharTest(TypeMixin, TestCase):
    data = [('one',), ('three',)]
    datatypes = ['varchar(10)']

class CharTest(TypeMixin, TestCase):
    data = [('one',), ('three',)]
    datatypes = ['char(5)']

    def cast(self, v):
        self.assertEqual(5, len(v))
        return v.strip()

class TextTest(TypeMixin, TestCase):
    data = [
        ('Fourscore and seven years ago our fathers set forth'
         'on this continent a new nation',),
        ('Python is a programming language that lets you work quickly'
         'and integrate systems more effectively.',),
    ]
    datatypes = ['text']

class ByteaTest(TypeMixin, TestCase):
    data = [
        ('\x02\xf3\x18\x44 alphabet',),
        ('The\x00hippopotamus\x00jumped\x00over\x00the\x00lazy\x00tower',)
    ,]
    datatypes = ['bytea']

    def cast(self, v):
        self.assertIsInstance(v, buffer)
        return str(v)

class TimestampTest(TypeMixin, TestCase):
    data = [(datetime.now(),), (datetime(1974, 8, 21, 6, 30),) ]
    datatypes = ['timestamp']

class TimestampTZTest(TypeMixin, TestCase):
    data = [
            (util.to_utc(datetime.now()),),
            (util.to_utc(datetime(1974, 8, 21, 6, 30)),)
        ]
    datatypes = ['timestamp with time zone']

    def cast(self, v):
        return util.to_utc(v)

class DateTest(TypeMixin, TestCase):
    data = [(date(2003, 5, 24),), (date.today(),)]
    datatypes = ['date']
