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

import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.CLOCK;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.SchemaProvider;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.entities.Rule;
import io.confluent.kafka.schemaregistry.client.rest.entities.RuleMode;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.FieldEncryptionExecutorTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "register-deks", mixinStandardHelpOptions = true,
    description = "Register and/or auto-rotate DEKs according to a specified data contract.",
    sortOptions = false, sortSynopsis = false)
public class RegisterDeks implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(RegisterDeks.class);

  private static final String DEFAULT_RULE_PARAM_PREFIX = "rule.executors._default_.param.";

  @Parameters(index = "0",
      description = "SR (Schema Registry) URL", paramLabel = "<url>")
  private String baseUrl;
  @Parameters(index = "1",
      description = "Subject", paramLabel = "<subject>")
  private String subject;
  @Parameters(index = "2", arity = "0..1", defaultValue = "-1",
      description = "Version, defaults to latest", paramLabel = "<version>")
  private int version;
  @Option(names = {"-X", "--property"},
      description = "Set configuration property.", paramLabel = "<prop=val>")
  private Map<String, String> configs;

  private Clock clock;

  public RegisterDeks() {
  }

  public Clock getClock() {
    return clock;
  }

  public void setClock(Clock clock) {
    this.clock = clock;
  }

  @Override
  public Integer call() throws Exception {
    Map<String, Object> configs = this.configs != null
        ? new HashMap<>(this.configs)
        : new HashMap<>();
    configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, baseUrl);
    if (clock != null) {
      configs.put(CLOCK, clock);
    }

    try (SchemaRegistryClient client = SchemaRegistryClientFactory.newClient(
        Collections.singletonList(baseUrl),
        1000,
        ImmutableList.of(new AvroSchemaProvider()),
        configs,
        Collections.emptyMap()
    )) {
      SchemaMetadata schemaMetadata = getSchemaMetadata(client);
      Optional<ParsedSchema> schema = parseSchema(schemaMetadata);
      if (!schema.isPresent()) {
        LOG.error("No schema found");
        return 1;
      }
      ParsedSchema parsedSchema = schema.get();
      if (parsedSchema.ruleSet() == null || parsedSchema.ruleSet().getDomainRules() == null) {
        LOG.info("No rules found");
        return 0;
      }
      List<Rule> rules = parsedSchema.ruleSet().getDomainRules();
      for (int i = 0; i < rules.size(); i++) {
        Rule rule = rules.get(i);
        if (rule.isDisabled() || !FieldEncryptionExecutor.TYPE.equals(rule.getType())) {
          continue;
        }
        processRule(configs, parsedSchema, rules, i, rule);
      }
      return 0;
    }
  }

  private SchemaMetadata getSchemaMetadata(SchemaRegistryClient client)
      throws IOException, RestClientException {
    SchemaMetadata schemaMetadata = version >= 0
        ? client.getSchemaMetadata(subject, version)
        : client.getLatestSchemaMetadata(subject);
    return schemaMetadata;
  }

  private void processRule(Map<String, Object> configs, ParsedSchema parsedSchema, List<Rule> rules,
      int i, Rule rule) throws RuleException, GeneralSecurityException {
    try (FieldEncryptionExecutor executor = new FieldEncryptionExecutor()) {
      Map<String, Object> ruleConfigs = configsWithoutPrefix(rule, configs);
      executor.configure(ruleConfigs);
      RuleContext ctx = new RuleContext(configs, null, parsedSchema,
          subject, null, null, null, null, false, RuleMode.WRITE, rule, i, rules);
      FieldEncryptionExecutorTransform transform = executor.newTransform(ctx);
      transform.getOrCreateDek(ctx, transform.isDekRotated() ? -1 : null);
    }
  }

  private Optional<ParsedSchema> parseSchema(SchemaMetadata schemaMetadata) throws Exception {
    SchemaProvider provider;
    switch (schemaMetadata.getSchemaType()) {
      case "AVRO":
        provider = new AvroSchemaProvider();
        break;
      case "JSON":
        provider = (SchemaProvider)
            Class.forName("io.confluent.kafka.schemaregistry.protobuf.ProtobufSchemaProvider")
                .getDeclaredConstructor()
                .newInstance();
        break;
      case "PROTOBUF":
        provider = (SchemaProvider)
            Class.forName("io.confluent.kafka.schemaregistry.json.JsonSchemaProvider")
                .getDeclaredConstructor()
                .newInstance();
        break;
      default:
        throw new IllegalArgumentException("Unsupported schema type "
            + schemaMetadata.getSchemaType());
    }
    return provider.parseSchema(new Schema(null, schemaMetadata), false, false);
  }

  private Map<String, Object> configsWithoutPrefix(Rule rule, Map<String, Object> configs) {
    // Add default params
    Map<String, Object> ruleConfigs = new HashMap<>(configs);
    for (Map.Entry<String, Object> entry: configs.entrySet()) {
      String name = entry.getKey();
      if (name.startsWith(DEFAULT_RULE_PARAM_PREFIX)) {
        ruleConfigs.put(name.substring(DEFAULT_RULE_PARAM_PREFIX.length()), entry.getValue());
      }
    }
    // Rule type specific params override default params
    String prefix = "rule.executors._ " + rule.getType() + "_.param.";
    for (Map.Entry<String, Object> entry: configs.entrySet()) {
      String name = entry.getKey();
      if (name.startsWith(prefix)) {
        ruleConfigs.put(name.substring(prefix.length()), entry.getValue());
      }
    }
    // Named params override rule type specific params and default params
    prefix = "rule.executors." + rule.getName() + ".param.";
    for (Map.Entry<String, Object> entry: configs.entrySet()) {
      String name = entry.getKey();
      if (name.startsWith(prefix)) {
        ruleConfigs.put(name.substring(prefix.length()), entry.getValue());
      }
    }
    return ruleConfigs;
  }

  public static void main(String[] args) {
    CommandLine commandLine = new CommandLine(new RegisterDeks());
    commandLine.setUsageHelpLongOptionsMaxWidth(30);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }
}