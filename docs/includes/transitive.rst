.. transitive

|sr-long| can check compatibility of a new schema against just the latest registered schema for that subject, or if configured as transitive then it checks against all previously registered schemas, not just the latest one.
For example, if there are three schemas for a subject that change in order `A`, `B`, and `C` then:

* `non-transitive` ensures compatibility between A <==> B and B <==> C
* `transitive` ensures compatibility between A <==> B and B <==> C and A <==> C

Note that the default compatibility type ``BACKWARD`` is the non-transitive type (i.e., it is not ``BACKWARD_TRANSITIVE``).
As a result, new schemas are checked for compatibility only against the latest schema.
