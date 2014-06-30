import calendar
import itertools
import struct
import sys

from cStringIO import StringIO
from datetime import date

from . import inspect, util

__all__ = ['CopyManager']

BINCOPY_HEADER = struct.pack('>11sii', b'PGCOPY\n\377\r\n\0', 0, 0)
BINCOPY_NULL = struct.pack('>i', -1)
BINCOPY_TRAILER = struct.pack('>h', -1)

def simple_formatter(fmt):
    size = struct.calcsize('>' + fmt)
    fmt_str = '>i' + fmt
    return lambda val: struct.pack(fmt_str, size, val)

def str_formatter(val):
    return struct.pack('>I', len(val)) + val

def maxsize_formatter(maxsize):
    def formatter(val):
        val = val[:maxsize]
        return struct.pack('>I', len(val)) + val
    return formatter

psql_epoch = 946684800
psql_epoch_date = date(2000, 1, 1)

def timestamp(dt):
    'get microseconds since 2000-01-01 00:00'
    # see http://stackoverflow.com/questions/2956886/
    dt = util.to_utc(dt)
    unix_timestamp = calendar.timegm(dt.timetuple())
    # timetuple doesn't maintain microseconds
    # see http://stackoverflow.com/a/14369386/519015
    val = ((unix_timestamp - psql_epoch) * 1e6) + dt.microsecond
    return struct.pack('>iq', 8, val)

def datestamp(d):
    'days since 2000-01-01'
    return struct.pack('>ii', 4, (d - psql_epoch_date).days)

def null(formatter):
    def nullcheck(val):
        if val is None:
            return BINCOPY_NULL
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
            f = type_formatters[coltype]
            if typemod > -1:
                f = f(typemod)
            if not notnull:
                f = null(f)
            self.formatters.append(f)

    def copy(self, data):
        datastream = StringIO()
        self.writestream(data, datastream)
        datastream.seek(0)
        self.copystream(datastream)

    def writestream(self, data, datastream):
        datastream.write(BINCOPY_HEADER)
        for record in data:
            datastream.write(struct.pack('>h', len(self.cols)))
            for formatter, val in itertools.izip(self.formatters, record):
                datastream.write(formatter(val))
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

