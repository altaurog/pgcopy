Extending
-----------

It is possible to add support for new datatypes by subclassing
:py:class:`pgcopy.CopyManager` with a ``type_formatters`` attribute.
See ``pgcopy.contrib.vector`` for an example.  But if you add support for a
new datatype, please open a PR so we can include it in pgcopy and make it
available for others!
