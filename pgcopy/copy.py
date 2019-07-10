import calendar
import functools
import os
import struct
import tempfile
import threading

from datetime import date

try:
    from itertools import izip as zip
except ImportError:
    pass

from psycopg2.extensions import encodings
from . import inspect, util

__all__ = ['CopyManager']

BINCOPY_HEADER = struct.pack('>11sii', b'PGCOPY\n\377\r\n\0', 0, 0)
BINCOPY_TRAILER = struct.pack('>h', -1)

MAX_INT64 = 0xFFFFFFFFFFFFFFFF


def simple_formatter(fmt):
    size = struct.calcsize('>' + fmt)
    return lambda val: ('i' + fmt, (size, val))

def str_formatter(val):
    size = len(val)
    return ('i%ss' % size, (size, val))

psql_epoch = 946684800
psql_epoch_date = date(2000, 1, 1)

def timestamp(dt):
    'get microseconds since 2000-01-01 00:00'
    # see http://stackoverflow.com/questions/2956886/
    dt = util.to_utc(dt)
    unix_timestamp = calendar.timegm(dt.timetuple())
    # timetuple doesn't maintain microseconds
    # see http://stackoverflow.com/a/14369386/519015
    val = ((unix_timestamp - psql_epoch) * 1000000) + dt.microsecond
    return ('iq', (8, val))

def datestamp(d):
    'days since 2000-01-01'
    return ('ii', (4, (d - psql_epoch_date).days))

def numeric(n):
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
        weight = 0
        while decdigits:
            if any(decdigits[:4]):
                break
            weight += 1
            del decdigits[:4]
        while decdigits:
            digits.insert(0, ndig(decdigits[:4]))
            del decdigits[:4]
        ndigits = len(digits)
        weight += nt.exponent // 4 + ndigits - 1
        sign = nt.sign * 0x4000
        dscale = -min(0, nt.exponent)
    data = [ndigits, weight, sign, dscale] + digits
    return ('ihhHH%dH' % ndigits, [2 * len(data)] + data)

def ndig(a):
    res = 0
    for i, d in enumerate(a):
        res += d * 10 ** i
    return res


def jsonb_formatter(val):
    size = len(val)
    # first char must me binary format of jsonb in postgresql
    return 'ib%is' % size, (size + 1, 1, val)


def uuid_formatter(guid):
    return 'i2Q', (16, (guid.int >> 64) & MAX_INT64, guid.int & MAX_INT64)


def array_formatter():
    """
    i   total size in bytes
    i   number of dimensions
    i   whether there are nulls or not
    i   element type (typelem)

    for each axis:
        i   length
        i   lower bound (when unraveled, 1-based, seems to always be 1)

    each element, unnested
    """


type_formatters = {
    'bool': simple_formatter('?'),
    'int2': simple_formatter('h'),
    'int4': simple_formatter('i'),
    'int8': simple_formatter('q'),
    'float4' : simple_formatter('f'),
    'float8': simple_formatter('d'),
    'varchar': str_formatter,
    'bpchar': str_formatter,
    'bytea': str_formatter,
    'text': str_formatter,
    'json': str_formatter,
    'jsonb': jsonb_formatter,
    'date': datestamp,
    'timestamp': timestamp,
    'timestamptz': timestamp,
    'numeric': numeric,
    'uuid': uuid_formatter,
}

def null(att, _, formatter):
    if not att.not_null:
        return lambda v: ('i', (-1,)) if v is None else formatter(v)
    message = 'null value in column "{}" not allowed'.format(att.attname)
    def nullcheck(v):
        if v is None:
            raise ValueError(message)
        return formatter(v)
    return nullcheck


def maxsize(att, _, formatter):
    if att.type_name not in ('varchar', 'bpchar'):
        return formatter
    def _maxsize(v):
        # postgres reports size + 4
        size = min(len(v), att.type_mod - 4) if att.type_mod >= 0 else len(v)
        return formatter(v[:size])
    return _maxsize


def encode(att, encoding, formatter):
    if att.type_name not in ('varchar', 'text', 'json'):
        return formatter
    def _encode(v):
        try:
            encf = v.encode
        except AttributeError:
            return formatter(v)
        else:
            return formatter(encf(encoding))
    return _encode


def get_formatter(att):
    try:
        return type_formatters[att.type_name]
    except KeyError:
        raise TypeError('type {} is not supported'.format(att.type_name))


class CopyManager(object):
    def __init__(self, conn, table, cols):
        self.conn = conn
        if '.' in table:
            self.schema, self.table = table.split('.', 1)
        else:
            self.schema, self.table = util.get_schema(conn, table), table
        self.cols = cols
        self.compile()

    def compile(self):
        self.formatters = []
        type_dict = inspect.get_types(self.conn, self.schema, self.table)
        encoding = encodings[self.conn.encoding]
        for column in self.cols:
            att = type_dict.get(column)
            if att is None:
                message = '"%s" is not a column of table "%s"."%s"'
                raise ValueError(message % (column, self.schema, self.table))
            funcs = [encode, maxsize, null]
            reducer = lambda f, mf: mf(att, encoding, f)
            f = functools.reduce(reducer, funcs, get_formatter(att))
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
        cmd = 'COPY "{0}"."{1}" ("{2}") FROM STDIN WITH BINARY'
        sql = cmd.format(self.schema, self.table, columns)
        cursor = self.conn.cursor()
        try:
            cursor.copy_expert(sql, datastream)
        except Exception as e:
            templ = "error doing binary copy into {0}.{1}:\n{2}"
            e.message = templ.format(self.schema, self.table, e)
            raise e
