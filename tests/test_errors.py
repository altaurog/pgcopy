import pytest
from pgcopy import CopyManager
from . import db

class TestErrors(db.TemporaryTable):
    datatypes = ['integer']
    def test_nosuchcolumn(self, conn, schema_table):
        col = self.cols[0] + '_does_not_exist'
        with pytest.raises(ValueError):
            CopyManager(conn, schema_table, [col])

    def test_notnull(self, conn, schema_table):
        bincopy = CopyManager(conn, schema_table, self.cols)
        with pytest.raises(ValueError):
            bincopy.copy([[None]])


class TestDroppedCol(db.TemporaryTable):
    datatypes = ['integer', 'integer']
    def test_dropped_col(self, conn, cursor, schema_table):
        sql = 'ALTER TABLE {} DROP COLUMN {}'
        cursor.execute(sql.format(schema_table, self.cols[1]))
        with pytest.raises(ValueError):
            CopyManager(conn, schema_table, self.cols)
