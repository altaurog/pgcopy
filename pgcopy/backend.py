"psycopg backends"
import importlib
import os

from .thread import RaisingThread


class Psycopg2Backend:
    def __init__(self, conn):
        self.conn = conn
        self.adaptor = importlib.import_module("psycopg2")
        self.adaptor.extras = importlib.import_module("psycopg2.extras")

    def get_encoding(self):
        encodings = self.adaptor.extensions.encodings
        return encodings[self.conn.encoding]

    def namedtuple_cursor(self):
        factory = self.adaptor.extras.NamedTupleCursor
        return self.conn.cursor(cursor_factory=factory)

    def copy(self, sql, fobject_factory):
        return Psycopg2Copy(self.conn, sql, fobject_factory)

    def threading_copy(self, sql):
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
        cursor = self.conn.cursor()
        cursor.copy_expert(self.sql, self.datastream)


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
        cursor = self.conn.cursor()
        cursor.copy_expert(self.sql, self.rstream)