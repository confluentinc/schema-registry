/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.encryption.tools;

import static org.junit.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import io.confluent.dekregistry.client.DekRegistryClient;
import io.confluent.dekregistry.client.DekRegistryClientFactory;
import io.confluent.dekregistry.client.MockDekRegistryClientFactory;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.Metadata;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleSet;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionProperties;
import io.confluent.kafka.schemaregistry.encryption.local.LocalFieldEncryptionProperties;
import io.confluent.kafka.schemaregistry.testutil.FakeClock;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.avro.Schema;
import org.junit.After;
import org.junit.Test;
import picocli.CommandLine;

public class RegisterDeksTest {

  private final FieldEncryptionProperties fieldEncryptionProps;
  private final SchemaRegistryClient schemaRegistry;
  private final DekRegistryClient dekRegistry;
  private final String topic;
  private final FakeClock fakeClock = new FakeClock();

  public RegisterDeksTest() throws Exception {
    topic = "test";
    fieldEncryptionProps = new LocalFieldEncryptionProperties(ImmutableList.of("rule1"));
    schemaRegistry = SchemaRegistryClientFactory.newClient(Collections.singletonList(
            "mock://"),
        1000,
        ImmutableList.of(new AvroSchemaProvider()),
        null,
        null
    );
    dekRegistry = DekRegistryClientFactory.newClient(Collections.singletonList(
            "mock://"),
        1000,
        100000,
        Collections.emptyMap(),
        null
    );
  }

  private Schema createUserSchema() {
    String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", "
        + "\"name\": \"User\","
        + "\"fields\": ["
        + "{\"name\": \"name\", \"type\": [\"null\", \"string\"], \"confluent:tags\": [\"PII\", \"PII3\"]},"
        + "{\"name\": \"name2\", \"type\": [\"null\", \"string\"], \"confluent:tags\": [\"PII2\"]},"
        + "{\"name\": \"age\", \"type\": [\"null\", \"int\"]}"
        + "]}";
    Schema.Parser parser = new Schema.Parser();
    Schema schema = parser.parse(userSchema);
    return schema;
  }

  @After
  public void tearDown() {
    MockSchemaRegistry.clear();
    MockDekRegistryClientFactory.clear();
  }

  @Test
  public void testRegisterDek() throws Exception {
    String subject = topic + "-value";
    String kekName = "kek1";
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"), null, null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata(kekName);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(subject, avroSchema);

    RegisterDeks app = new RegisterDeks();
    CommandLine cmd = new CommandLine(app);

    int exitCode = cmd.execute(
        "mock://", subject, "--property", "rule.executors._default_.param.secret=mysecret");
    assertEquals(0, exitCode);

    Dek dek = dekRegistry.getDekVersion(kekName, subject, -1, null, false);
    assertEquals(kekName, dek.getKekName());
  }

  @Test
  public void testRotateDek() throws Exception {
    String subject = topic + "rotate-value";
    String kekName = "kek2";
    AvroSchema avroSchema = new AvroSchema(createUserSchema());
    Rule rule = new Rule("rule1", null, null, null,
        FieldEncryptionExecutor.TYPE, ImmutableSortedSet.of("PII"),
        ImmutableMap.of("encrypt.dek.expiry.days", "1"),
        null, null, null, false);
    RuleSet ruleSet = new RuleSet(Collections.emptyList(), ImmutableList.of(rule));
    Metadata metadata = getMetadata(kekName);
    avroSchema = avroSchema.copy(metadata, ruleSet);
    schemaRegistry.register(subject, avroSchema);

    RegisterDeks app = new RegisterDeks();
    app.setClock(fakeClock);
    CommandLine cmd = new CommandLine(app);

    int exitCode = cmd.execute(
        "mock://", subject, "--property", "rule.executors._default_.param.secret=mysecret");
    assertEquals(0, exitCode);

    Dek dek = dekRegistry.getDekVersion(kekName, subject, -1, null, false);
    assertEquals(kekName, dek.getKekName());
    assertEquals(1, dek.getVersion());

    exitCode = cmd.execute(
        "mock://", subject, "--property", "rule.executors._default_.param.secret=mysecret");
    assertEquals(0, exitCode);

    dek = dekRegistry.getDekVersion(kekName, subject, -1, null, false);
    assertEquals(kekName, dek.getKekName());
    assertEquals(1, dek.getVersion());

    // Advance 2 days
    fakeClock.advance(2, ChronoUnit.DAYS);

    exitCode = cmd.execute(
        "mock://", subject, "--property", "rule.executors._default_.param.secret=mysecret");
    assertEquals(0, exitCode);

    dek = dekRegistry.getDekVersion(kekName, subject, -1, null, false);
    assertEquals(kekName, dek.getKekName());
    assertEquals(2, dek.getVersion());
  }

  protected Metadata getMetadata(String kekName) {
    Map<String, String> properties = new HashMap<>();
    properties.put(FieldEncryptionExecutor.ENCRYPT_KEK_NAME, kekName);
    properties.put(FieldEncryptionExecutor.ENCRYPT_KMS_TYPE, fieldEncryptionProps.getKmsType());
    properties.put(FieldEncryptionExecutor.ENCRYPT_KMS_KEY_ID, fieldEncryptionProps.getKmsKeyId());
    return getMetadata(properties);
  }

  protected Metadata getMetadata(Map<String, String> properties) {
    return new Metadata(Collections.emptyMap(), properties, Collections.emptySet());
  }
}

