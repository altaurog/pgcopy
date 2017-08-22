from unittest import TestCase
from pgcopy import CopyManager
from . import db

class TestErrors(db.TemporaryTable, TestCase):
    datatypes = ['integer']
    def test_nosuchcolumn(self):
        conn = self.conn
        col = self.cols[0] + '_does_not_exist'
        args = (conn, self.schema_table, [col])
        self.assertRaises(ValueError, CopyManager, *args)
