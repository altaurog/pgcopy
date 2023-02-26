"""
Decorating and re-raising an exception is a pain in Python 2
Python 3 "fixes" this pain by introducing new syntax!
"""

import sys

if sys.version_info[0] == 2:
    from .py2 import raise_from
else:
    from .py3 import raise_from
