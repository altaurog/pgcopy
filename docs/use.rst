Use
----
To use pgcopy, instantiate a copy manager for your table,
then pass it some data:

.. include:: ../README.rst
   :start-after: quickstart-start
   :end-before: home-end

.. autoclass:: pgcopy.CopyManager

   .. automethod:: copy(data[, fobject_factory])
   .. automethod:: threading_copy
