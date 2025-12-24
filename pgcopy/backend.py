"psycopg backends"

import codecs
import collections
import contextlib
import importlib
import os

from .errors import UnsupportedConnectionError
from .thread import RaisingThread


def for_connection(conn):
    sources = [cls.__module__.split(".")[0] for cls in conn.__class__.mro()]
    if "psycopg2" in sources:
        return Psycopg2Backend(conn)
    if "psycopg" in sources:
        return Psycopg3Backend(conn)
    if "pgdb" in sources:
        return PyGreSQLBackend(conn)
    if "pg8000" in sources:
        return Pg8000Backend(conn)
    message = f"{conn.__class__.__name__} is not a supported connection type"
    raise UnsupportedConnectionError(message)


def copy_sql(schema, table, columns):
    column_list = '", "'.join(columns)
    cmd = 'COPY "{0}"."{1}" ("{2}") FROM STDIN WITH BINARY'
    return cmd.format(schema, table, column_list)


class Psycopg2Backend:
    def __init__(self, conn):
        self.conn = conn
        self.adaptor = importlib.import_module("psycopg2")
        self.adaptor.extras = importlib.import_module("psycopg2.extras")

    def get_encoding(self):
        return self.adaptor.extensions.encodings[self.conn.encoding]

    def namedtuple_cursor(self):
        factory = self.adaptor.extras.NamedTupleCursor
        return self.conn.cursor(cursor_factory=factory)

    def copy(self, schema, table, columns, fobject_factory):
        sql = copy_sql(schema, table, columns)
        return Psycopg2Copy(self.conn, sql, fobject_factory)

    def threading_copy(self, schema, table, columns):
        sql = copy_sql(schema, table, columns)
        return Psycopg2ThreadingCopy(self.conn, sql)


class Psycopg2Copy:
    def __init__(self, conn, sql, fobject_factory):
        self.conn = conn
        self.sql = sql
        self.datastream = fobject_factory()

    def __enter__(self):
        return self.datastream

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.datastream.seek(0)
            self.copystream()
        self.datastream.close()

    def copystream(self):
        with self.conn.cursor() as cur:
            cur.copy_expert(self.sql, self.datastream)


class Psycopg2ThreadingCopy:
    def __init__(self, conn, sql):
        self.conn = conn
        self.sql = sql
        r_fd, w_fd = os.pipe()
        self.rstream = os.fdopen(r_fd, "rb")
        self.wstream = os.fdopen(w_fd, "wb")

    def __enter__(self):
        self.copy_thread = RaisingThread(target=self.copystream)
        self.copy_thread.start()
        return self.wstream

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.wstream.close()
        self.copy_thread.join()

    def copystream(self):
        with self.conn.cursor() as cur:
            cur.copy_expert(self.sql, self.rstream)


class Psycopg3Backend:
    def __init__(self, conn):
        self.conn = conn
        self.adaptor = importlib.import_module("psycopg")

    def get_encoding(self):
        return self.conn.info.encoding

    def namedtuple_cursor(self):
        factory = self.adaptor.rows.namedtuple_row
        return self.conn.cursor(row_factory=factory)

    @contextlib.contextmanager
    def copy(self, schema, table, columns, _):
        sql = copy_sql(schema, table, columns)
        with self.conn.cursor() as cur:
            with cur.copy(sql) as copy:
                yield copy

    @contextlib.contextmanager
    def threading_copy(self, schema, table, columns):
        sql = copy_sql(schema, table, columns)
        with self.conn.cursor() as cur:
            with cur.copy(sql) as copy:
                yield copy


class PyGreSQLBackend:
    def __init__(self, conn):
        self.conn = conn

    def get_encoding(self):
        with self.conn.cursor() as cur:
            cur.execute("SHOW client_encoding")
            row = cur.fetchone()
            return codecs.lookup(row.client_encoding).name

    def namedtuple_cursor(self):
        return self.conn.cursor()

    def copy(self, schema, table, columns, fobject_factory):
        return PyGreSQLCopy(self.conn, schema, table, columns, fobject_factory)


class PyGreSQLCopy:
    def __init__(self, conn, schema, table, columns, fobject_factory):
        self.conn = conn
        self.table = f"{schema}.{table}"
        self.columns = columns
        self.datastream = fobject_factory()

    def __enter__(self):
        return self.datastream

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.datastream.seek(0)
            self.copystream()
        self.datastream.close()

    def copystream(self):
        with self.conn.cursor() as cur:
            cur.copy_from(
                self.datastream,
                self.table,
                format="binary",
                columns=self.columns,
            )


class PyGreSQLThreadingCopy:
    "but it doesn’t work"

    def __init__(self, conn, schema, table, columns):
        self.conn = conn
        self.table = f"{schema}.{table}"
        self.columns = columns
        r_fd, w_fd = os.pipe()
        self.rstream = os.fdopen(r_fd, "rb")
        self.wstream = os.fdopen(w_fd, "wb")

    def __enter__(self):
        self.copy_thread = RaisingThread(target=self.copystream)
        self.copy_thread.start()
        return self.wstream

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.wstream.close()
        self.copy_thread.join()

    def copystream(self):
        with self.conn.cursor() as cur:
            cur.copy_from(
                self.rstream,
                self.table,
                format="binary",
                columns=self.columns,
            )


class Pg8000Backend:
    NamedTupleCursor = None

    def __init__(self, conn):
        self.conn = conn

    def get_encoding(self):
        with contextlib.closing(self.namedtuple_cursor()) as cur:
            cur.execute("SHOW client_encoding")
            row = cur.fetchone()
            return codecs.lookup(row.client_encoding).name

    def namedtuple_cursor(self):
        if not Pg8000Backend.NamedTupleCursor:
            cur = self.conn.cursor()
            Cursor = cur.__class__
            cur.close()

            class NamedTupleCursor(Cursor):
                def __next__(self):
                    val = super().__next__()
                    context = self._context
                    if context is None:
                        return val  # raised an error already
                    rowclass = getattr(context, "_pgcopy_row_class", None)
                    if not rowclass:
                        columns = context.columns
                        if columns is None or len(columns) == 0:
                            return val  # probably also raised an error
                        column_names = [col["name"] for col in columns]
                        rowclass = collections.namedtuple("Row", column_names)
                        context._pgcopy_row_class = rowclass
                    return rowclass(*val)

            Pg8000Backend.NamedTupleCursor = NamedTupleCursor
        return Pg8000Backend.NamedTupleCursor(self.conn)

    def copy(self, schema, table, columns, fobject_factory):
        sql = copy_sql(schema, table, columns)
        return Pg8000Copy(self.conn, sql, fobject_factory)


class Pg8000Copy:
    def __init__(self, conn, sql, fobject_factory):
        self.conn = conn
        self.sql = sql
        self.datastream = fobject_factory()

    def __enter__(self):
        return self.datastream

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.datastream.seek(0)
            self.copystream()
        self.datastream.close()

    def copystream(self):
        cur = self.conn.cursor()
        cur.execute(self.sql, stream=self.datastream)
        cur.close()
