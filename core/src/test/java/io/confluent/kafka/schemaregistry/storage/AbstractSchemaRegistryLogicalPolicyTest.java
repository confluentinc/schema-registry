/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.ParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.SimpleParsedSchemaHolder;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Verifies the {@code CompatibilityPolicy.LOGICAL} branch inside
 * {@link AbstractSchemaRegistry#isCompatibleWithPrevious} -- that the logical checks run only under
 * that policy and are layered additively on top of the native compatibility check.
 *
 * <p>Uses {@code CALLS_REAL_METHODS} to exercise the real method body while bypassing the registry
 * constructor, the pattern established by {@code AbstractSchemaRegistryNullEncoderTest}.
 */
class AbstractSchemaRegistryLogicalPolicyTest {

  // old has a field that new drops. Native Avro treats a field removal as backward-compatible,
  // but the logical Iceberg check rejects FIELD_DELETED -- so this pair isolates the additive
  // logical behavior from the native check.
  private static final String RECORD_A_B =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
          + "{\"name\":\"a\",\"type\":\"int\"},"
          + "{\"name\":\"b\",\"type\":\"int\"}]}";
  private static final String RECORD_A =
      "{\"type\":\"record\",\"name\":\"R\",\"fields\":["
          + "{\"name\":\"a\",\"type\":\"int\"}]}";

  private static List<String> check(String policy, ParsedSchema newSchema,
      List<ParsedSchemaHolder> previous) {
    AbstractSchemaRegistry registry = mock(AbstractSchemaRegistry.class, CALLS_REAL_METHODS);
    Config config = new Config();
    config.setCompatibilityLevel("BACKWARD");
    config.setCompatibilityPolicy(policy);
    return registry.isCompatibleWithPrevious(config, newSchema, previous);
  }

  @Test
  void logicalPolicyRunsTheLogicalChecks() {
    List<String> errors = check(
        "LOGICAL",
        new AvroSchema(RECORD_A),
        List.of(new SimpleParsedSchemaHolder(new AvroSchema(RECORD_A_B))));
    // FIELD_DELETED is a logical-checker-only Rule (native Avro treats a dropped field as
    // backward-compatible and never reports this), so its presence isolates the LOGICAL branch.
    assertTrue(errors.stream().anyMatch(e -> e.contains("FIELD_DELETED")),
        "expected a logical finding under LOGICAL policy: " + errors);
  }

  @Test
  void nonLogicalPolicyDoesNotRunTheLogicalChecks() {
    List<String> errors = check(
        "STRICT",
        new AvroSchema(RECORD_A),
        List.of(new SimpleParsedSchemaHolder(new AvroSchema(RECORD_A_B))));
    assertTrue(errors.stream().noneMatch(e -> e.contains("FIELD_DELETED")),
        "logical checks must not run unless policy is LOGICAL: " + errors);
  }

  @Test
  void logicalPolicyPassesAValidCompatibleSchema() {
    // Identical schema, so both native and logical see no change -- no findings at all.
    List<String> errors = check(
        "LOGICAL",
        new AvroSchema(RECORD_A_B),
        List.of(new SimpleParsedSchemaHolder(new AvroSchema(RECORD_A_B))));
    assertTrue(errors.isEmpty(), errors.toString());
  }

  @Test
  void allErrorTypeFindingsStayContiguous() {
    // new(A_B) adding required field 'b' (no default) trips both checks: native Avro reports
    // READER_FIELD_MISSING_DEFAULT_VALUE, and the logical checker reports REQUIRED_FIELD_ADDED.
    // The native check also appends an {oldSchema...} context entry after its own finding -- that
    // context entry (and the trailing {validateFields...} debug entry) must not land in between
    // the two checks' {errorType...} findings and split them apart.
    List<String> errors = check(
        "LOGICAL",
        new AvroSchema(RECORD_A_B),
        List.of(new SimpleParsedSchemaHolder(new AvroSchema(RECORD_A))));

    List<Integer> errorTypeIndexes = new ArrayList<>();
    for (int i = 0; i < errors.size(); i++) {
      if (errors.get(i).contains("errorType")) {
        errorTypeIndexes.add(i);
      }
    }
    assertTrue(errorTypeIndexes.size() >= 2, "expected findings from both checks: " + errors);
    int first = errorTypeIndexes.get(0);
    int last = errorTypeIndexes.get(errorTypeIndexes.size() - 1);
    assertTrue(last - first + 1 == errorTypeIndexes.size(),
        "errorType findings are not contiguous: " + errors);
  }
}
