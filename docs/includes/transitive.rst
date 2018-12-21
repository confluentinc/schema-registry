.. transitive

|sr-long| can check compatibility of a new schema against just the latest registered schema for that subject, or if configured as transitive then it checks against all previously registered schemas, not just the latest one.
For example, if there are three schemas for a subject that change in order `X-2`, `X-1`, and `X` then:

* `transitive` ensures compatibility between `X-2` <==> `X-1` and `X-1` <==> `X` and `X-2` <==> `X`
* `non-transitive` ensures compatibility between `X-2` <==> `X-1` and `X-1` <==> `X`, but not necessarily `X-2` <==> `X`

Note that the default compatibility type ``BACKWARD`` is the non-transitive type (i.e., it is not ``BACKWARD_TRANSITIVE``).
As a result, new schemas are checked for compatibility only against the latest schema.
