import struct

from .. import copy, util


def vector_formatter(val):
    info = util.array_info(val)
    ndim, lengths = info[0], info[1:]
    if ndim != 1:
        raise ValueError("{} is not a 1D array type".format(val))

    # https://github.com/pgvector/pgvector/blob/587e9ba97c1cb057117bc9b081c0170b5013f8d8/src/vector.c#L402-L419
    fmt = ">hh" + "f" * lengths[0]
    data = [lengths[0], 0, *val]

    return copy.str_formatter(struct.pack(fmt, *data))


class CopyManager(copy.CopyManager):
    "Add support for vector"
    type_formatters = {"vector": vector_formatter}
