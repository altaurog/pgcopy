import calendar
import functools
import itertools
import os
import struct
import sys
import tempfile
import threading

from datetime import date

from . import inspect, util

__all__ = ['CopyManager']

BINCOPY_HEADER = struct.pack('>11sii', b'PGCOPY\n\377\r\n\0', 0, 0)
BINCOPY_TRAILER = struct.pack('>h', -1)

def simple_formatter(fmt):
    size = struct.calcsize('>' + fmt)
    return lambda _, val: ('i' + fmt, (size, val))

def str_formatter(_, val):
    size = len(val)
    return ('i%ss' % size, (size, val))

def maxsize_formatter(maxsize, val):
    val = val[:maxsize]
    size = len(val)
    return ('i%ss' % size, (size, val))

psql_epoch = 946684800
psql_epoch_date = date(2000, 1, 1)

def timestamp(_, dt):
    'get microseconds since 2000-01-01 00:00'
    # see http://stackoverflow.com/questions/2956886/
    dt = util.to_utc(dt)
    unix_timestamp = calendar.timegm(dt.timetuple())
    # timetuple doesn't maintain microseconds
    # see http://stackoverflow.com/a/14369386/519015
    val = ((unix_timestamp - psql_epoch) * 1000000) + dt.microsecond
    return ('iq', (8, val))

def datestamp(_, d):
    'days since 2000-01-01'
    return ('ii', (4, (d - psql_epoch_date).days))

def null(formatter):
    def nullcheck(val):
        if val is None:
            return ('i', (-1,))
        return formatter(val)
    return nullcheck

type_formatters = {
    'bool': simple_formatter('?'),
    'int2': simple_formatter('h'),
    'int4': simple_formatter('i'),
    'int8': simple_formatter('q'),
    'float4' : simple_formatter('f'),
    'float8': simple_formatter('d'),
    'varchar': maxsize_formatter,
    'bpchar': maxsize_formatter,
    'bytea': str_formatter,
    'text': str_formatter,
    'date': datestamp,
    'timestamp': timestamp,
    'timestamptz': timestamp,
}

class CopyManager(object):
    def __init__(self, conn, table, cols):
        self.conn = conn
        self.table = table
        self.cols = cols
        self.compile()

    def compile(self):
        self.formatters = []
        type_dict = inspect.get_types(self.conn, self.table)
        for column in self.cols:
            type_info = type_dict.get(column)
            if type_info is None:
                message = '"%s" is not a column of table "%s"'
                raise ValueError(message % (column, self.table))
            coltype, typemod, notnull = type_info
            f = functools.partial(type_formatters[coltype], typemod)
            if not notnull:
                f = null(f)
            self.formatters.append(f)

    def copy(self, data, fobject_factory=tempfile.TemporaryFile):
        datastream = fobject_factory()
        self.writestream(data, datastream)
        datastream.seek(0)
        self.copystream(datastream)
        datastream.close()

    def threading_copy(self, data):
        r_fd, w_fd = os.pipe()
        rstream = os.fdopen(r_fd, 'rb')
        wstream = os.fdopen(w_fd, 'wb')
        copy_thread = threading.Thread(target=self.copystream, args=(rstream,))
        copy_thread.start()
        self.writestream(data, wstream)
        wstream.close()
        copy_thread.join()

    def writestream(self, data, datastream):
        datastream.write(BINCOPY_HEADER)
        count = len(self.cols)
        for record in data:
            fmt = ['>h']
            rdat = [count]
            for formatter, val in itertools.izip(self.formatters, record):
                f, d = formatter(val)
                fmt.append(f)
                rdat.extend(d)
            datastream.write(struct.pack(''.join(fmt), *rdat))
        datastream.write(BINCOPY_TRAILER)

    def copystream(self, datastream):
        columns = '", "'.join(self.cols)
        sql = """COPY "{0}" ("{1}")
                FROM STDIN WITH BINARY""".format(self.table, columns)
        cursor = self.conn.cursor()
        try:
            cursor.copy_expert(sql, datastream)
        except Exception, e:
            message = "error doing binary copy into %s:\n%s" % (self.table, e.message)
            raise type(e), type(e)(message), sys.exc_info()[2]
