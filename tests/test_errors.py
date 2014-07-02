from unittest import TestCase
from pgcopy import CopyManager
from . import db

class TestErrors(db.TemporaryTable, TestCase):
    datatypes = ['integer']
    def test_nosuchcolumn(self):
        conn = self.conn
        col = self.cols[0] + '_does_not_exist'
        self.assertRaises(ValueError, CopyManager, conn, self.table, [col])
