/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * SQL CHECK → CEL translator. Takes a parsed SQL CHECK expression
 * (postgres-derived subset) plus a {@link
 * io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintValidationContext}
 * and emits a CEL expression string suitable for evaluation by any cel-go /
 * cel-java engine. The translator is purely text-emitting — it does not bundle
 * a CEL evaluator.
 *
 * <h2>Layered architecture</h2>
 *
 * <p>The translator runs three layers in order, designed so each layer catches
 * a distinct class of error:
 *
 * <ol>
 *   <li><b>Pre-emit hand-coded validators</b> ({@link
 *       io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintValidator})
 *       — SQL-semantic checks that CEL itself can't see (e.g.
 *       {@code col IS NULL} on a non-nullable column is dead code,
 *       {@code col + 1} where {@code col} is a string is meant as concat in
 *       SQL even though CEL accepts {@code string+string}). These checks
 *       enforce SQL intent before emit.</li>
 *
 *   <li><b>Emit</b> ({@link
 *       io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintEmitter},
 *       {@link
 *       io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintFunctions},
 *       {@link
 *       io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintPatterns})
 *       — cascade walkers turn the parse tree into CEL fragments. Type
 *       resolution lives in {@link
 *       io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintResolver}
 *       and is shared between validators and emitters.</li>
 *
 *   <li><b>Post-emit strict cel-java type-check</b> ({@link
 *       io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintCelChecker},
 *       {@link
 *       io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintTypeProvider})
 *       — every emitted CEL is run through cel-java's strict type-checker
 *       with the schema's actual column types declared (not {@code dyn}). Any
 *       type/overload mismatch the hand-coded validators missed becomes a
 *       parse-time {@link
 *       io.confluent.kafka.schemaregistry.type.logical.ValidationException}
 *       instead of a runtime CEL error downstream.</li>
 * </ol>
 *
 * <h2>What each layer is responsible for</h2>
 *
 * <p><b>ConstraintValidator</b> covers checks strict can't subsume:
 * <ul>
 *   <li>Column-ref resolution (typos vs unknown bindings)</li>
 *   <li>Literal NULL operands to NULL-rejecting funcs (GREATEST/LEAST/
 *       COALESCE/NULLIF) — strict's NULL-branch unification is fuzzier</li>
 *   <li>BETWEEN with NULL or {@code IS NULL} as a bound — emits valid CEL
 *       but flips semantics</li>
 *   <li>Literal index bounds (negative indices, out-of-range)</li>
 *   <li>Macro arity / lambda var name shape</li>
 *   <li>Date/time literal format and numeric overflow</li>
 *   <li>SQL intent on {@code +}/{@code -}/{@code *}/{@code %} (numeric
 *       only — strict accepts {@code string+string} as concat)</li>
 *   <li>Cross-type equality (CEL accepts {@code int == string} as
 *       always-false; SQL rejects)</li>
 *   <li>Ordering ops on non-orderable types (cel-java has {@code less_bool},
 *       cel-go does not — restored for cross-engine portability)</li>
 *   <li>IN-list element-type homogeneity (CEL accepts {@code list<dyn>})</li>
 * </ul>
 *
 * <p><b>ConstraintCelChecker</b> covers everything else — every type and
 * overload error in the emitted CEL. Catches the long tail of bugs that
 * hand-coded validators previously had to enumerate one-by-one (compound
 * type errors through arithmetic, function-result types, has() argument
 * shape, missing CEL overloads, etc.).
 *
 * <h2>Cross-engine concerns</h2>
 *
 * <p>The strict checker uses Google cel-java ({@code dev.cel}) but the
 * emitted CEL may be evaluated by cel-go in production. A few overloads
 * differ between the two implementations:
 * <ul>
 *   <li>cel-java has {@code less_bool}, cel-go does not — handled by the
 *       hand-coded orderable-type check</li>
 *   <li>Format validators ({@code isEmail}, {@code isHostname}, etc.) and
 *       the cel-go StringsExt methods ({@code upperAscii}, {@code substring},
 *       etc.) are declared in
 *       {@code ConstraintCelChecker#commonDeclarations} —
 *       production cel-go runtimes must register equivalents</li>
 * </ul>
 *
 * <h2>Validation context</h2>
 *
 * <p>{@link
 * io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintValidationContext}
 * carries the column/field bindings and signals placement (column-level vs
 * table-level CHECK). Placement determines emit shape:
 * <ul>
 *   <li>Column-level: {@code this} is the field's value; {@code col} in
 *       SQL collapses to {@code this} in CEL</li>
 *   <li>Table-level: {@code this} is the surrounding struct; {@code col}
 *       in SQL emits as {@code this.col}</li>
 * </ul>
 *
 * <p>The strict checker reads the placement from vctx and declares
 * {@code this} accordingly: a primitive type for column-level, a synthetic
 * struct type ({@link
 * io.confluent.kafka.schemaregistry.type.logical.constraint.ConstraintTypeProvider})
 * for table-level.
 *
 * <h2>Column-level skip-on-null contract</h2>
 *
 * <p>Column-level CHECKs are evaluated under a runtime contract: <b>the CEL
 * is not invoked when the column value is null/missing/unset</b>. This rule
 * applies uniformly across all formats (Avro nullable union, proto3 unset
 * field, JSON missing property), giving column-level constraints
 * format-portable semantics matching SQL CHECK's "TRUE or UNKNOWN passes"
 * spec.
 *
 * <p>Two consequences for users:
 * <ul>
 *   <li><b>No null-guarding required.</b> Built-in functions and operators
 *       on the column ({@code LENGTH(name) > 0}, {@code name.matches(...)},
 *       {@code name + 'x' = ...}, etc.) are safe at column level — the
 *       runtime won't pass null and the operations can't crash on null
 *       inputs.</li>
 *   <li><b>Null-aware tests on the bare column are dead code.</b>
 *       {@code <col> IS NULL} / {@code <col> IS NOT NULL} and
 *       {@code COALESCE(<col>, ...)} at column level are rejected by the
 *       validator. Use {@code NOT NULL} at the schema level for nullability
 *       declarations, or move the test to a table-level CHECK where
 *       {@code has(this.x)} provides format-aware presence checks.</li>
 * </ul>
 *
 * <p>The rule applies only to the bare column reference at column level.
 * Nested struct fields ({@code <col>.field IS NULL}), array indexing
 * ({@code <col>[0] IS NULL}), and macro iter-vars
 * ({@code EVERY(<col>, t, t IS NULL)}) keep their existing null-awareness
 * — only the outermost {@code this} is guaranteed non-null.
 */
package io.confluent.kafka.schemaregistry.type.logical.constraint;
