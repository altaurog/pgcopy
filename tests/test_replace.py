import contextlib

import pytest
from pgcopy import Replace, util

from . import db, db_connection

@pytest.mark.skipif(db_connection.IS_DSQL, reason="tests not supported on dsql")
class TemporaryTable(db.TemporaryTable):
    id_col = False


def tuplist(iterable):
    return [tuple(row) for row in iterable]


class TestRenameReplace(TemporaryTable):
    datatypes = ["integer"]
    mixed_case = False

    def test_rename_replace(self, conn, cursor, schema):
        viewsql = "CREATE VIEW v AS SELECT a + 1 FROM {}"
        cursor.execute(viewsql.format(self.table))
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        cursor.executemany(sql.format(self.table), [(1,), (2,)])
        xform = lambda s: s + "_old"
        with util.RenameReplace(conn, self.table, xform) as temp:
            cursor.executemany(sql.format(temp), [(36,), (72,)])
        cursor.execute("SELECT * FROM {}".format(self.table))
        assert tuplist(cursor) == [(36,), (72,)]
        cursor.execute("SELECT * FROM v")
        assert tuplist(cursor) == [(37,), (73,)]
        cursor.execute("SELECT * FROM {}".format(self.table + "_old"))
        assert tuplist(cursor) == [(1,), (2,)]


class TestReplaceFallbackSchema(TemporaryTable):
    datatypes = ["integer"]
    mixed_case = False

    def test_fallback_schema_honors_search_path(
        self, conn, cursor, schema, schema_table
    ):
        if not super().tempschema:
            pytest.skip("This test currently requires temp schema")
        cursor.execute(self.create_sql(tempschema=False))
        cursor.execute("SET search_path TO {}".format(schema))
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        with Replace(conn, self.table) as temp:
            cursor.execute(sql.format(temp), (1,))
        cursor.execute("SELECT * FROM {}".format(schema_table))
        assert tuplist(cursor) == [(1,)]


class TestReplaceDefault(TemporaryTable):
    """
    Defaults are set on temp table immediately.
    """

    mixed_case = False
    null = ""
    datatypes = [
        "integer",
        "integer DEFAULT 3",
    ]

    def test_replace_with_default(self, conn, cursor, schema_table):
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        with Replace(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (1,))
        cursor.execute("SELECT * FROM {}".format(schema_table))
        assert tuplist(cursor) == [(1, 3)]


@pytest.fixture
def replace_raises(conn, schema_table, integrity_error):
    @contextlib.contextmanager
    def _replace_raises():
        """
        Wrap Replace context manager and assert
        exception is thrown on context exit
        """
        r = Replace(conn, schema_table)
        yield r.__enter__()
        with pytest.raises(integrity_error):
            r.__exit__(None, None, None)

    return _replace_raises

class TestReplaceNotNull(TemporaryTable):
    mixed_case = False
    null = ""
    datatypes = [
        "integer",
        "integer NOT NULL",
    ]

    def test_replace_not_null(self, replace_raises, cursor):
        """
        Not-null constraint is added on exit
        """
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        with replace_raises() as temp:
            cursor.execute(sql.format(temp), (1,))
            cursor.execute("SELECT * FROM {}".format(temp))
            assert tuplist(cursor) == [(1, None)]


class TestReplaceConstraint(TemporaryTable):
    mixed_case = False
    null = ""
    datatypes = [
        "integer CHECK (a > 5)",
    ]

    def test_replace_constraint(self, replace_raises, cursor):
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        with replace_raises() as temp:
            cursor.execute(sql.format(temp), (1,))
            cursor.execute("SELECT * FROM {}".format(temp))
            assert tuplist(cursor) == [(1,)]


class TestReplaceNamedConstraint(TemporaryTable):
    mixed_case = False
    null = ""
    datatypes = [
        "integer CONSTRAINT asize CHECK (a > 5)",
    ]

    def test_replace_constraint_no_name_conflict(self, conn, schema_table):
        with Replace(conn, schema_table) as temp:
            pass
        with Replace(conn, schema_table) as temp:
            pass


class TestReplaceUniqueIndex(TemporaryTable):
    mixed_case = False
    null = ""
    datatypes = [
        "integer UNIQUE",
    ]

    def test_replace_unique_index(self, replace_raises, cursor):
        """
        Not-null constraint is added on exit
        """
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        with replace_raises() as temp:
            cursor.execute(sql.format(temp), (1,))
            cursor.execute(sql.format(temp), (1,))
            cursor.execute("SELECT * FROM {}".format(temp))
            assert tuplist(cursor) == [(1,), (1,)]


class TestReplaceView(TemporaryTable):
    mixed_case = False
    datatypes = ["integer"]

    def test_replace_with_view(self, conn, cursor, schema_table):
        viewsql = "CREATE VIEW v AS SELECT a + 1 FROM {}"
        cursor.execute(viewsql.format(schema_table))
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        with Replace(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (1,))
        cursor.execute("SELECT * FROM v")
        assert tuplist(cursor) == [(2,)]


class TestReplaceViewMultiSchema(TemporaryTable):
    mixed_case = False
    tempschema = False
    datatypes = ["integer"]

    def test_replace_view_in_different_schema(self, conn, cursor, schema_table):
        viewsql = "CREATE SCHEMA ns CREATE VIEW v AS SELECT a + 1 FROM {}"
        cursor.execute(viewsql.format(schema_table))
        sql = 'INSERT INTO public.{} ("a") VALUES (%s)'
        with Replace(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (1,))
        cursor.execute("SELECT * FROM ns.v")
        assert tuplist(cursor) == [(2,)]


class TestReplaceTrigger(TemporaryTable):
    mixed_case = False
    null = ""
    datatypes = [
        "integer",
        "integer",
    ]

    def test_replace_with_trigger(self, conn, cursor, schema_table):
        cursor.execute("CREATE SCHEMA fs")
        cursor.execute(
            """
            CREATE FUNCTION fs.table_ins() RETURNS trigger AS $table_ins$
            BEGIN
                NEW.b := NEW.a * 4;
                RETURN NEW;
            END;
        $table_ins$ LANGUAGE plpgsql;
        """
        )
        trigsql = """
            CREATE TRIGGER table_on_ins BEFORE INSERT ON {}
            FOR EACH ROW EXECUTE PROCEDURE fs.table_ins()
        """
        cursor.execute(trigsql.format(schema_table))
        sql = 'INSERT INTO {} ("a", "b") VALUES (%s, %s)'
        with Replace(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (1, 1))
        cursor.execute(sql.format(schema_table), (2, 1))
        cursor.execute("SELECT * FROM {}".format(schema_table))
        assert tuplist(cursor) == [(1, 1), (2, 8)]


class TestReplaceSequence(TemporaryTable):
    mixed_case = False
    null = ""
    datatypes = [
        "integer",
        "serial",
    ]

    def test_replace_with_sequence(self, conn, cursor, schema_table):
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        cursor.executemany(sql.format(schema_table), [(10,), (20,)])
        with Replace(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (30,))
            cursor.execute(sql.format(schema_table), (40,))
        cursor.execute(sql.format(schema_table), (40,))
        cursor.execute("SELECT * FROM {}".format(schema_table))
        assert tuplist(cursor) == [(30, 3), (40, 5)]
