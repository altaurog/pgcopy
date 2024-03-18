import pytest
from pgcopy import CopyManager

from . import db


class TestErrors(db.TemporaryTable):
    datatypes = ["integer"]

    def test_nosuchcolumn(self, conn, schema):
        col = self.cols[0] + "_does_not_exist"
        msg = '"{}" is not a column of table "{}"."{}"'
        with pytest.raises(ValueError, match=msg.format(col, schema, self.table)):
            CopyManager(conn, self.table, [col])

    def test_notnull(self, conn, schema_table):
        bincopy = CopyManager(conn, schema_table, self.cols)
        message = 'null value in column "{}" not allowed'.format(self.cols[0])
        with pytest.raises(ValueError, match=message):
            bincopy.copy([[None]])


class TestFormatterDiagnostic(db.TemporaryTable):
    datatypes = ["varchar"]

    def test_formatting_diagnostic(self, conn):
        copymgr = CopyManager(conn, self.table, self.cols)
        message = "error formatting value 23 for column {}".format(self.cols[0])
        with pytest.raises(ValueError, match=message):
            copymgr.copy([[23]])


class TestDroppedCol(db.TemporaryTable):
    datatypes = ["integer", "integer"]

    def test_dropped_col(self, conn, cursor, schema):
        sql = 'ALTER TABLE "{}" DROP COLUMN "{}"'
        col = self.cols[1]
        cursor.execute(sql.format(self.table, col))
        msg = '"{}" is not a column of table "{}"."{}"'
        with pytest.raises(ValueError, match=msg.format(col, schema, self.table)):
            CopyManager(conn, self.table, self.cols)
