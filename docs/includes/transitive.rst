.. transitive

Transitive compatibility checking is important once you have more than two versions of a schema for a given subject.
If compatibility is configured as transitive, then it checks compatibility of a new schema against all previously registered schemas; otherwise, it checks compatibility of a new schema only against the latest schema.

For example, if there are three schemas for a subject that change in order `X-2`, `X-1`, and `X` then:

* transitive: ensures compatibility between `X-2` <==> `X-1` and `X-1` <==> `X` and `X-2` <==> `X`
* non-transitive: ensures compatibility between `X-2` <==> `X-1` and `X-1` <==> `X`, but not necessarily `X-2` <==> `X`

Refer to an `example of schema changes <https://github.com/confluentinc/schema-registry/issues/209>`__ which are incrementally compatible, but not transitively so.

Note that |sr-long| default compatibility type ``BACKWARD`` is the non-transitive type (i.e., it is not ``BACKWARD_TRANSITIVE``).
As a result, new schemas are checked for compatibility only against the latest schema.

