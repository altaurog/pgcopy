import pytest
from pgcopy import CopyManager
from . import db

class TestErrors(db.TemporaryTable):
    datatypes = ['integer']
    def test_nosuchcolumn(self):
        conn = self.conn
        col = self.cols[0] + '_does_not_exist'
        with pytest.raises(ValueError):
            CopyManager(conn, self.schema_table, [col])
