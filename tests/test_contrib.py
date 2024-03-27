import json

import pgcopy.contrib.vector

from .test_datatypes import TypeMixin


class TestVector(TypeMixin):
    copy_manager_class = pgcopy.contrib.vector.CopyManager
    extensions = ["vector"]
    datatypes = ["vector"]
    data = [
        ((-1.5, 0, 2.3),),
    ]

    def cast(self, v):
        return tuple(json.loads(v))
