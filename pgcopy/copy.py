import calendar
import functools
import os
import struct
import sys
import tempfile
import threading

from datetime import date

try:
    from itertools import izip as zip
except ImportError:
    pass

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

def numeric(_, n):
    """
    NBASE = 1000
    ndigits = total number of base-NBASE digits
    weight = base-NBASE weight of first digit
    sign = 0x0000 if positive, 0x4000 if negative, 0xC000 if nan
    dscale = decimal digits after decimal place
    """
    try:
        nt = n.as_tuple()
    except AttributeError:
        raise TypeError('numeric field requires Decimal value (got %r)' % n)
    digits = []
    if isinstance(nt.exponent, str):
        # NaN, Inf, -Inf
        ndigits = 0
        weight = 0
        sign = 0xC000
        dscale = 0
    else:
        decdigits = list(reversed(nt.digits + (nt.exponent % 4) * (0,)))
        while decdigits:
            if any(decdigits[:4]):
                break
            del decdigits[:4]
        while decdigits:
            digits.insert(0, ndig(decdigits[:4]))
            del decdigits[:4]
        ndigits = len(digits)
        weight = nt.exponent // 4 + ndigits - 1
        sign = nt.sign * 0x4000
        dscale = -min(0, nt.exponent)
    data = [ndigits, weight, sign, dscale] + digits
    return ('ihhHH%dH' % ndigits, [2 * len(data)] + data)

def ndig(a):
    res = 0
    for i, d in enumerate(a):
        res += d * 10 ** i
    return res

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
    'numeric': numeric,
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
            for formatter, val in zip(self.formatters, record):
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
        except Exception as e:
            e.message = "error doing binary copy into %s:\n%s" % (self.table, e.message)
            raise e
