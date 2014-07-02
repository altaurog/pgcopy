from unittest import TestCase
from pgcopy import CopyManager
from . import db

class TestErrors(db.TemporaryTable, TestCase):
    datatypes = ['integer']
    def test_nosuchcolumn(self):
        conn = self.conn
        self.assertRaises(ValueError, CopyManager, conn, 'int_notnull', ['num'])
