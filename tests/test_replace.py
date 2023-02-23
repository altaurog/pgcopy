import contextlib
import os

import psycopg2
import psycopg2.sql
import pytest
from pgcopy import Replace
from pgcopy import util
from . import db


class TestRenameReplace(db.TemporaryTable):
    datatypes = ['integer']

    def test_rename_replace(self, conn, cursor, schema):
        if not schema.startswith("pg_"):
            cursor.execute(
                psycopg2.sql.SQL(
                "CREATE SCHEMA IF NOT EXISTS {}").format(
                    psycopg2.sql.Identifier(schema)
                )
            )
        schema_qualified_table = psycopg2.sql.Identifier(schema, self.table)
        schema_qualified_table_v = psycopg2.sql.Identifier(schema, "v")
        viewsql = psycopg2.sql.SQL("CREATE VIEW {} AS SELECT a + 1 FROM {}")
        cursor.execute(viewsql.format(schema_qualified_table_v,
                                      schema_qualified_table))
        sql = psycopg2.sql.SQL('INSERT INTO {} ("a") VALUES (%s)')
        cursor.executemany(sql.format(schema_qualified_table), [(1,), (2,)])
        xform = lambda s: (s[1:-1] if len(s) > 1 and ((s[0], s[-1]) in (('"', '"'), ("'", "'"))) else s) + '_old'
        with util.RenameReplace(conn, self.table, xform) as temp:
            cursor.executemany(sql.format(temp), [(36,), (72,)])
        cursor.execute(psycopg2.sql.SQL('SELECT * FROM {}').format(schema_qualified_table))
        assert list(cursor) == [(36,), (72,)]
        cursor.execute(psycopg2.sql.SQL('SELECT * FROM {}').format(schema_qualified_table_v))
        assert list(cursor) == [(37,), (73,)]
        cursor.execute(psycopg2.sql.SQL('SELECT * FROM {}').format(
            psycopg2.sql.Identifier(schema, self.table + '_old')
        ))
        assert list(cursor) == [(1,), (2,)]


class TestReplaceFallbackSchema(db.TemporaryTable):
    datatypes = ['integer']

    def test_fallback_schema_honors_search_path(self, conn, cursor, schema, schema_table):
        cursor.execute(self.create_sql(tempschema=False))
        cursor.execute(psycopg2.sql.SQL('SET search_path TO {}').format(schema))
        sql = psycopg2.sql.SQL('INSERT INTO {} ("a") VALUES (%s)')
        with Replace(conn, self.table) as temp:
            cursor.execute(sql.format(temp), (1,))
        cursor.execute(psycopg2.sql.SQL('SELECT * FROM {}').format(schema_table))
        assert list(cursor) == [(1,)]


class TestReplaceDefault(db.TemporaryTable):
    """
    Defaults are set on temp table immediately.
    """
    null = ''
    datatypes = [
        'integer',
        'integer DEFAULT 3',
    ]

    def test_replace_with_default(self, conn, cursor, schema_table):
        sql = psycopg2.sql.SQL('INSERT INTO {} ("a") VALUES (%s)')
        with Replace(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (1,))
        cursor.execute(psycopg2.sql.SQL('SELECT * FROM {}').format(schema_table))
        assert list(cursor) == [(1, 3)]


@contextlib.contextmanager
def replace_raises(conn, table, exc=psycopg2.IntegrityError):
    """
    Wrap Replace context manager and assert
    exception is thrown on context exit
    """
    r = Replace(conn, table)
    yield r.__enter__()
    with pytest.raises(exc):
        r.__exit__(None, None, None)


class TestReplaceNotNull(db.TemporaryTable):
    null = ''
    datatypes = [
        'integer',
        'integer NOT NULL',
    ]

    def test_replace_not_null(self, conn, cursor, schema_table):
        """
        Not-null constraint is added on exit
        """
        sql = psycopg2.sql.SQL('INSERT INTO {} ("a") VALUES (%s)')
        with replace_raises(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (1,))
            cursor.execute(psycopg2.sql.SQL('SELECT * FROM {}').format(temp))
            assert list(cursor) == [(1, None)]


class TestReplaceConstraint(db.TemporaryTable):
    null = ''
    datatypes = [
        'integer CHECK (a > 5)',
    ]

    def test_replace_constraint(self, conn, cursor, schema_table):
        sql = psycopg2.sql.SQL('INSERT INTO {} ("a") VALUES (%s)')
        with replace_raises(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (1,))
            cursor.execute(psycopg2.sql.SQL('SELECT * FROM {}').format(temp))
            assert list(cursor) == [(1,)]


class TestReplaceNamedConstraint(db.TemporaryTable):
    null = ''
    datatypes = [
        'integer CONSTRAINT asize CHECK (a > 5)',
    ]

    def test_replace_constraint_no_name_conflict(self, conn, schema_table):
        with Replace(conn, schema_table) as temp:
            pass
        with Replace(conn, schema_table) as temp:
            pass


class TestReplaceUniqueIndex(db.TemporaryTable):
    null = ''
    datatypes = [
        'integer UNIQUE',
    ]

    def test_replace_unique_index(self, conn, cursor, schema_table):
        """
        Not-null constraint is added on exit
        """
        sql = psycopg2.sql.SQL('INSERT INTO {} ("a") VALUES (%s)')
        with replace_raises(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (1,))
            cursor.execute(sql.format(temp), (1,))
            cursor.execute(psycopg2.sql.SQL('SELECT * FROM {}').format(temp))
            assert list(cursor) == [(1,), (1,)]


class TestReplaceView(db.TemporaryTable):
    datatypes = ['integer']

    def test_replace_with_view(self, conn, cursor, schema_table):
        viewsql = psycopg2.sql.SQL("CREATE VIEW v AS SELECT a + 1 FROM {}")
        cursor.execute(viewsql.format(schema_table))
        sql = psycopg2.sql.SQL('INSERT INTO {} ("a") VALUES (%s)')
        with Replace(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (1,))
        cursor.execute(psycopg2.sql.SQL('SELECT * FROM v'))
        assert list(cursor) == [(2,)]


class TestReplaceViewMultiSchema(db.TemporaryTable):
    tempschema = False
    datatypes = ['integer']

    def test_replace_view_in_different_schema(self, conn, cursor, schema_table):
        viewsql = psycopg2.sql.SQL('CREATE SCHEMA ns CREATE VIEW v AS SELECT a + 1 FROM {}')
        cursor.execute(viewsql.format(schema_table))
        sql = psycopg2.sql.SQL('INSERT INTO public.{} ("a") VALUES (%s)')
        with Replace(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (1,))
        cursor.execute(psycopg2.sql.SQL('SELECT * FROM ns.v'))
        assert list(cursor) == [(2,)]


class TestReplaceTrigger(db.TemporaryTable):
    null = ''
    datatypes = [
        'integer',
        'integer',
    ]

    def test_replace_with_trigger(self, conn, cursor, schema_table):
        cursor.execute(psycopg2.sql.SQL("CREATE SCHEMA fs"))
        cursor.execute(psycopg2.sql.SQL("""
            CREATE FUNCTION fs.table_ins() RETURNS trigger AS $table_ins$
            BEGIN
                NEW.b := NEW.a * 4;
                RETURN NEW;
            END;
        $table_ins$ LANGUAGE plpgsql;
        """))
        trigsql = psycopg2.sql.SQL("""
            CREATE TRIGGER table_on_ins BEFORE INSERT ON {}
            FOR EACH ROW EXECUTE PROCEDURE fs.table_ins()
        """)
        cursor.execute(trigsql.format(schema_table))
        sql = psycopg2.sql.SQL('INSERT INTO {} ("a", "b") VALUES (%s, %s)')
        with Replace(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (1, 1))
        cursor.execute(sql.format(schema_table), (2, 1))
        cursor.execute(psycopg2.sql.SQL('SELECT * FROM {}').format(schema_table))
        assert list(cursor) == [(1, 1), (2, 8)]


class TestReplaceSequence(db.TemporaryTable):
    null = ''
    datatypes = [
        'integer',
        'serial',
    ]

    def test_replace_with_sequence(self, conn, cursor, schema_table):
        sql = psycopg2.sql.SQL('INSERT INTO {} ("a") VALUES (%s)')
        cursor.executemany(sql.format(schema_table), [(10,), (20,)])
        with Replace(conn, schema_table) as temp:
            cursor.execute(sql.format(temp), (30,))
            cursor.execute(sql.format(schema_table), (40,))
        cursor.execute(sql.format(schema_table), (40,))
        cursor.execute(psycopg2.sql.SQL('SELECT * FROM {}').format(schema_table))
        assert list(cursor) == [(30, 3), (40, 5)]
