import calendar
import functools
import os
import struct
import tempfile
import uuid
from datetime import date, datetime

try:
    from itertools import izip as zip
except ImportError:
    pass

from psycopg2.extensions import encodings

from . import errors, inspect, util
from .thread import RaisingThread

__all__ = ["CopyManager"]

BINCOPY_HEADER = struct.pack(">11sii", b"PGCOPY\n\377\r\n\0", 0, 0)
BINCOPY_TRAILER = struct.pack(">h", -1)

MAX_INT64 = 0xFFFFFFFFFFFFFFFF


def simple_formatter(fmt):
    size = struct.calcsize(">" + fmt)
    return lambda val: ("i" + fmt, (size, val))


def str_formatter(val):
    size = len(val)
    return ("i%ss" % size, (size, val))


psql_epoch = 946684800
psql_epoch_date = date(2000, 1, 1)


def timestamp(dt):
    "get microseconds since 2000-01-01 00:00"
    # see http://stackoverflow.com/questions/2956886/
    dt = util.to_utc(dt)
    unix_timestamp = calendar.timegm(dt.timetuple())
    # timetuple doesn't maintain microseconds
    # see http://stackoverflow.com/a/14369386/519015
    val = ((unix_timestamp - psql_epoch) * 1000000) + dt.microsecond
    return ("iq", (8, val))


def time_formatter(t):
    "get microseconds since 2000-01-01 00:00"
    t = util.to_utc_time(t)
    dt = datetime.combine(psql_epoch_date, t)
    return timestamp(dt)


def datestamp(d):
    "days since 2000-01-01"
    return ("ii", (4, (d - psql_epoch_date).days))


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
        raise TypeError("numeric field requires Decimal value (got %r)" % n)
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
    return ("ihhHH%dH" % ndigits, [2 * len(data)] + data)


def ndig(a):
    res = 0
    for i, d in enumerate(a):
        res += d * 10**i
    return res


def jsonb_formatter(val):
    size = len(val)
    # first char must me binary format of jsonb in postgresql
    return "ib%is" % size, (size + 1, 1, val)


def uuid_formatter(guid):
    if isinstance(guid, str):
        guid = uuid.UUID(guid)
    return "i2Q", (16, (guid.int >> 64) & MAX_INT64, guid.int & MAX_INT64)


type_formatters = {
    "bool": simple_formatter("?"),
    "int2": simple_formatter("h"),
    "int4": simple_formatter("i"),
    "int8": simple_formatter("q"),
    "float4": simple_formatter("f"),
    "float8": simple_formatter("d"),
    "varchar": str_formatter,
    "bpchar": str_formatter,
    "bytea": str_formatter,
    "text": str_formatter,
    "json": str_formatter,
    "jsonb": jsonb_formatter,
    "date": datestamp,
    "time": time_formatter,
    "timestamp": timestamp,
    "timestamptz": timestamp,
    "numeric": numeric,
    "uuid": uuid_formatter,
}


def null_formatter(formatter):
    return lambda v: ("i", (-1,)) if v is None else formatter(v)


def array_formatter(typelem, formatter, val):
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
    info = util.array_info(val)
    ndim, lengths = info[0], info[1:]
    if ndim == 0:
        raise ValueError("{} is not an array type".format(val))
    elems = list(util.array_iter(val))
    fmt = [">3i{}i".format(2 * ndim)]
    data = [ndim, None in elems, typelem] + [1] * ndim * 2
    data[3::2] = lengths
    for f, d in map(null_formatter(formatter), elems):
        fmt.append(f)
        data.extend(d)
    return str_formatter(struct.pack("".join(fmt), *data))


def null(att, _, formatter):
    if not att.not_null:
        return null_formatter(formatter)
    message = 'null value in column "{}" not allowed'.format(att.attname)

    def nullcheck(v):
        if v is None:
            raise ValueError(message)
        return formatter(v)

    return nullcheck


def array(att, _, formatter):
    if att.type_category != "A":
        return formatter
    return lambda v: array_formatter(att.typelem, formatter, v)


def maxsize(att, _, formatter):
    if att.type_name not in ("varchar", "bpchar"):
        return formatter

    def _maxsize(v):
        # postgres reports size + 4
        size = min(len(v), att.type_mod - 4) if att.type_mod >= 0 else len(v)
        return formatter(v[:size])

    return _maxsize


def encode(att, encoding, formatter):
    is_text_type = att.type_name in ("varchar", "text", "json")
    is_enum_type = att.type_category == "E"
    if not (is_text_type or is_enum_type):
        return formatter

    def _encode(v):
        try:
            encf = v.encode
        except AttributeError:
            return formatter(v)
        else:
            return formatter(encf(encoding))

    return _encode


def diagnostic(att, encoding, formatter):
    template = "error formatting value {} for column {}"

    def f(v):
        try:
            return formatter(v)
        except Exception as exc:
            message = template.format(v, att.attname)
            errors.raise_from(ValueError, message, exc)

    return f


class CopyManager(object):
    """
    Facility for bulk-loading data using binary copy.

    Inspects the database on instantiation for the column types.

    :param conn: a database connection
    :type conn: psycopg2 connection

    :param table: the table name.  Schema may be specified using dot notation: ``schema.table``.
    :type table: str

    :param cols: columns in the table into which to copy data
    :type cols: iterable of str

    :raises ValueError: if the table or columns do not exist.
    """

    type_formatters = {}

    def __init__(self, conn, table, cols):
        self._type_formatters = {
            **type_formatters,
            **self.type_formatters,
        }
        self.conn = conn
        if "." in table:
            self.schema, self.table = table.split(".", 1)
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
            funcs = [encode, maxsize, array, diagnostic, null]
            reducer = lambda f, mf: mf(att, encoding, f)
            f = functools.reduce(reducer, funcs, self.get_formatter(att))
            self.formatters.append(f)

    def get_formatter(self, att):
        if att.type_category == "E":
            return str_formatter
        try:
            return self._type_formatters[att.type_name]
        except KeyError:
            raise TypeError("type {} is not supported".format(att.type_name))

    def copy(self, data, fobject_factory=tempfile.TemporaryFile):
        """
        Copy data into the database using a temporary file.

        :param data: the data to be inserted
        :type data: iterable of iterables

        :param fobject_factory: a tempfile factory
        :type fobject_factory: function

        Data is serialized first in its entirety and then sent to the database.
        By default, a temporary file on disk is used.  If you have enough memory,
        you can get a slight performance benefit with in-memory storage::

            from io import BytesIO
            mgr.copy(records, BytesIO)

        For very large datasets, serialization can be done directly to the
        database connection using :meth:`threading_copy`.

        In most circumstances, however, data transfer over the network and
        db processing take significantly more time than writing and reading
        a temporary file on a local disk.

        ``ValueError`` is raised if a null value is provided for a column
        with non-null constraint.
        """
        datastream = fobject_factory()
        self.writestream(data, datastream)
        datastream.seek(0)
        self.copystream(datastream)
        datastream.close()

    def threading_copy(self, data):
        """
        Copy data, serializing directly to the database.

        :param data: the data to be inserted
        :type data: iterable of iterables
        """
        r_fd, w_fd = os.pipe()
        rstream = os.fdopen(r_fd, "rb")
        wstream = os.fdopen(w_fd, "wb")
        copy_thread = RaisingThread(target=self.copystream, args=(rstream,))
        copy_thread.start()
        self.writestream(data, wstream)
        wstream.close()
        copy_thread.join()

    def writestream(self, data, datastream):
        datastream.write(BINCOPY_HEADER)
        count = len(self.cols)
        for record in data:
            fmt = [">h"]
            rdat = [count]
            for formatter, val in zip(self.formatters, record):
                f, d = formatter(val)
                fmt.append(f)
                rdat.extend(d)
            datastream.write(struct.pack("".join(fmt), *rdat))
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
