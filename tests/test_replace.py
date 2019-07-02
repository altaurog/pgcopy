import contextlib
import psycopg2.errors
import pytest
from pgcopy import Replace
from . import db

class TestReplaceDefault(db.TemporaryTable):
    """
    Defaults are set on temp table immediately.
    """
    temp = ''
    null = ''
    datatypes = [
        'integer',
        'integer DEFAULT 3',
    ]

    def test_replace_with_default(self):
        cursor = self.conn.cursor()
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        with Replace(self.conn, self.table) as temp:
            cursor.execute(sql.format(temp), (1,))
        cursor.execute('SELECT * FROM {}'.format(self.table))
        assert list(cursor) == [(1, 3)]


@contextlib.contextmanager
def replace(conn, table, exc=psycopg2.errors.IntegrityError):
    """
    Wrap Replace context manager and assert
    exception is thrown on context exit
    """
    r = Replace(conn, table)
    yield r.__enter__()
    with pytest.raises(exc):
        r.__exit__(None, None, None)


class TestReplaceNotNull(db.TemporaryTable):
    temp = ''
    null = ''
    datatypes = [
        'integer',
        'integer NOT NULL',
    ]

    def test_replace_not_null(self):
        """
        Not-null constraint is added on exit
        """
        cursor = self.conn.cursor()
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        with replace(self.conn, self.table) as temp:
            cursor.execute(sql.format(temp), (1,))
            cursor.execute('SELECT * FROM {}'.format(temp))
            assert list(cursor) == [(1, None)]


class TestReplaceConstraint(db.TemporaryTable):
    temp = ''
    null = ''
    datatypes = [
        'integer CHECK (a > 5)',
    ]

    def test_replace_constraint(self):
        cursor = self.conn.cursor()
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        with replace(self.conn, self.table) as temp:
            cursor.execute(sql.format(temp), (1,))
            cursor.execute('SELECT * FROM {}'.format(temp))
            assert list(cursor) == [(1,)]


class TestReplaceNamedConstraint(db.TemporaryTable):
    temp = ''
    null = ''
    datatypes = [
        'integer CONSTRAINT asize CHECK (a > 5)',
    ]

    def test_replace_constraint_no_name_conflict(self):
        with Replace(self.conn, self.table) as temp:
            pass
        with Replace(self.conn, self.table) as temp:
            pass


class TestReplaceUniqueIndex(db.TemporaryTable):
    temp = ''
    null = ''
    datatypes = [
        'integer UNIQUE',
    ]

    def test_replace_unique_index(self):
        """
        Not-null constraint is added on exit
        """
        cursor = self.conn.cursor()
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        with replace(self.conn, self.table) as temp:
            cursor.execute(sql.format(temp), (1,))
            cursor.execute(sql.format(temp), (1,))
            cursor.execute('SELECT * FROM {}'.format(temp))
            assert list(cursor) == [(1,), (1,)]


class TestReplaceView(db.TemporaryTable):
    temp = ''
    null = ''
    datatypes = [
        'integer',
    ]

    def test_replace_with_view(self):
        cursor = self.conn.cursor()
        viewsql = "CREATE VIEW v AS SELECT a + 1 FROM {}"
        cursor.execute(viewsql.format(self.table))
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        with Replace(self.conn, self.table) as temp:
            cursor.execute(sql.format(temp), (1,))
        cursor.execute('SELECT * FROM v')
        assert list(cursor) == [(2,)]


class TestReplaceTrigger(db.TemporaryTable):
    temp = ''
    null = ''
    datatypes = [
        'integer',
        'integer',
    ]

    def test_replace_with_trigger(self):
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE FUNCTION table_ins() RETURNS trigger AS $table_ins$
            BEGIN
                NEW.b := NEW.a * 4;
                RETURN NEW;
            END;
        $table_ins$ LANGUAGE plpgsql;
        """)
        trigsql = """
            CREATE TRIGGER table_on_ins BEFORE INSERT ON {}
            FOR EACH ROW EXECUTE PROCEDURE table_ins()
        """
        cursor.execute(trigsql.format(self.table))
        sql = 'INSERT INTO {} ("a", "b") VALUES (%s, %s)'
        with Replace(self.conn, self.table) as temp:
            cursor.execute(sql.format(temp), (1, 1))
        cursor.execute(sql.format(self.table), (2, 1))
        cursor.execute('SELECT * FROM {}'.format(self.table))
        assert list(cursor) == [(1, 1), (2, 8)]


class TestReplaceSequence(db.TemporaryTable):
    temp = ''
    null = ''
    datatypes = [
        'integer',
        'serial',
    ]

    def test_replace_with_sequence(self):
        cursor = self.conn.cursor()
        sql = 'INSERT INTO {} ("a") VALUES (%s)'
        cursor.executemany(sql.format(self.table), [(10,), (20,)])
        with Replace(self.conn, self.table) as temp:
            cursor.execute(sql.format(temp), (30,))
            cursor.execute(sql.format(self.table), (40,))
        cursor.execute(sql.format(self.table), (40,))
        cursor.execute('SELECT * FROM {}'.format(self.table))
        assert list(cursor) == [(30, 3), (40, 5)]
