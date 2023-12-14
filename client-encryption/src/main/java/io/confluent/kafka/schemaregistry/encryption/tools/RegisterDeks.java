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
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor;
import io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.FieldEncryptionExecutorTransform;
import io.confluent.kafka.schemaregistry.rules.RuleContext;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;
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

  private static final String RULE_PARAM_PREFIX = "rule.executors._default_.param.";

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

  public RegisterDeks() {
  }

  @Override
  public Integer call() throws Exception {
    Map<String, String> configs = this.configs != null
        ? new HashMap<>(this.configs)
        : new HashMap<>();
    configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, baseUrl);

    try (SchemaRegistryClient client = SchemaRegistryClientFactory.newClient(
        Collections.singletonList(baseUrl),
        1000,
        ImmutableList.of(new AvroSchemaProvider()),
        configs,
        Collections.emptyMap()
    )) {
      SchemaMetadata schemaMetadata = client.getSchemaMetadata(subject, version);
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
      try (FieldEncryptionExecutor executor = new FieldEncryptionExecutor()) {
        Map<String, Object> ruleConfigs = configs.entrySet().stream()
            .collect(Collectors.toMap(e -> stripPrefix(e.getKey()), Entry::getValue));
        executor.configure(ruleConfigs);
        List<Rule> rules = parsedSchema.ruleSet().getDomainRules();
        for (int i = 0; i < rules.size(); i++) {
          Rule rule = rules.get(i);
          if (rule.isDisabled() || !FieldEncryptionExecutor.TYPE.equals(rule.getType())) {
            continue;
          }
          RuleContext ctx = new RuleContext(configs, null, parsedSchema,
              subject, null, null, null, null, false, RuleMode.WRITE, rule, i, rules);
          FieldEncryptionExecutorTransform transform = executor.newTransform(ctx);
          transform.getOrCreateDek(ctx, transform.isDekRotated() ? -1 : null);
        }
      }
      return 0;
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

  private static String stripPrefix(String name) {
    return name.startsWith(RULE_PARAM_PREFIX)
        ? name.substring(RULE_PARAM_PREFIX.length())
        : name;
  }

  public static void main(String[] args) {
    CommandLine commandLine = new CommandLine(new RegisterDeks());
    commandLine.setUsageHelpLongOptionsMaxWidth(30);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }
}

