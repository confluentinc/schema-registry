.. compatibility list

* ``BACKWARD`` (default): consumers using the new schema can read data written by producers using the latest registered schema
* ``BACKWARD_TRANSITIVE``: consumers using the new schema can read data written by producers using all previously registered schemas
* ``FORWARD``: consumers using the latest registered schema can read data written by producers using the new schema
* ``FORWARD_TRANSITIVE``: consumers using all previousely registered schemas can read data written by producers using the new schema
* ``FULL``: the new schema is forward and backward compatible with the latest registered schema
* ``FULL_TRANSITIVE``: the new schema is forward and backward compatible with all previously registered schemas
* ``NONE``: schema compatibility checks are disabled
