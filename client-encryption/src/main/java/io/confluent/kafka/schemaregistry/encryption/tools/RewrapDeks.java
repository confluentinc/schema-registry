/*
 * Copyright 2025 Confluent Inc.
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

import static io.confluent.kafka.schemaregistry.encryption.FieldEncryptionExecutor.EMPTY_AAD;

import com.google.crypto.tink.Aead;
import io.confluent.dekregistry.client.CachedDekRegistryClient.DekId;
import io.confluent.dekregistry.client.DekRegistryClient;
import io.confluent.dekregistry.client.DekRegistryClientFactory;
import io.confluent.dekregistry.client.rest.entities.Dek;
import io.confluent.dekregistry.client.rest.entities.Kek;
import io.confluent.kafka.schemaregistry.encryption.tink.AeadWrapper;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

@Command(name = "rewrap-deks", mixinStandardHelpOptions = true,
    description = "Rewrap DEKs using the latest version of the KEK.",
    sortOptions = false, sortSynopsis = false)
public class RewrapDeks implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(RewrapDeks.class);

  private static final String DEFAULT_RULE_PARAM_PREFIX = "rule.executors._default_.param.";

  @Parameters(index = "0",
      description = "SR (Schema Registry) URL", paramLabel = "<url>")
  private String baseUrl;
  @Parameters(index = "1",
      description = "KEK name", paramLabel = "<kekName>")
  private String kekName;
  @Parameters(index = "2", arity = "0..1", defaultValue = "true",
      description = "Subject, defaults to all subjects", paramLabel = "<subject>")
  private String subject;
  @Parameters(index = "3", arity = "0..1", defaultValue = "true",
      description = "Include deleted DEKs, defaults to true", paramLabel = "<includeDeleted>")
  private boolean includeDeleted;
  @Option(names = {"-X", "--property"},
      description = "Set configuration property.", paramLabel = "<prop=val>")
  private Map<String, String> configs;

  public RewrapDeks() {
  }

  @Override
  public Integer call() throws Exception {
    Map<String, Object> configs = this.configs != null
        ? new HashMap<>(this.configs)
        : new HashMap<>();
    configs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, baseUrl);
    Map<String, Object> ruleConfigs = configsWithoutPrefix(configs);
    configs.putAll(ruleConfigs);

    try (DekRegistryClient client = DekRegistryClientFactory.newClient(
        Collections.singletonList(baseUrl),
        10000,
        -1,
        configs,
        Collections.emptyMap()
    )) {
      Kek kek = client.getKek(kekName, includeDeleted);
      List<String> subjects = subject != null
          ? Collections.singletonList(subject)
          : client.listDeks(kekName, includeDeleted);

      for (String subject : subjects) {
        for (DekFormat algorithm : DekFormat.values()) {
          List<Integer> versions = client.listDekVersions(
              kekName,
              subject,
              algorithm,
              includeDeleted
          );
          for (Integer version : versions) {
            Dek dek = client.getDekVersion(
                kekName,
                subject,
                version,
                algorithm,
                includeDeleted
            );
            LOG.info("Rewrapping dek for subject " + subject + ", algorithm "
                + algorithm + ", version " + version + ", deleted " + dek.getDeleted());
            rewrapDek(configs, client, kek, dek);
          }
        }
      }
      return 0;
    }
  }

  private Dek rewrapDek(Map<String, Object> configs, DekRegistryClient client, Kek kek, Dek dek)
      throws GeneralSecurityException {
    DekId dekId = new DekId(dek.getKekName(), dek.getSubject(), dek.getVersion(),
        dek.getAlgorithm(), Boolean.TRUE.equals(dek.getDeleted()));
    String encryptedDekStr = null;
    if (!kek.isShared()) {
      Map<String, Object> aeadConfigs = new HashMap<>(configs);
      aeadConfigs.putAll(kek.getKmsProps());
      Aead aead = new AeadWrapper(aeadConfigs, kek.getKmsType(), kek.getKmsKeyId());
      // Decrypt with old KEK version
      byte[] rawDek = aead.decrypt(dek.getEncryptedKeyMaterialBytes(), EMPTY_AAD);
      // Encrypt with latest KEK version
      byte[] encryptedDek = aead.encrypt(rawDek, EMPTY_AAD);
      encryptedDekStr =
          new String(Base64.getEncoder().encode(encryptedDek), StandardCharsets.UTF_8);
      LOG.info("Converted previous encrypted key material from '" + dek.getEncryptedKeyMaterial()
          + "' to new encrypted key material '" + encryptedDekStr + "'");
    }

    return storeDekToRegistry(client, dekId, encryptedDekStr);
  }

  private Dek storeDekToRegistry(
      DekRegistryClient client, DekId key, String encryptedDekStr)
      throws GeneralSecurityException {
    try {
      Dek dek = client.createDek(
          key.getKekName(), key.getSubject(), key.getVersion(),
          key.getDekFormat(), encryptedDekStr, key.isLookupDeleted(), true);
      LOG.info("Successfully rewrapped dek for subject " + key.getSubject()
          + ", algorithm " + key.getDekFormat() + ", version " + key.getVersion());
      return dek;
    } catch (Exception e) {
      throw new GeneralSecurityException("Could not register dek for kek " + key.getKekName()
          + ", subject " + key.getSubject(), e);
    }
  }

  private Map<String, Object> configsWithoutPrefix(Map<String, Object> configs) {
    // Add default params
    Map<String, Object> ruleConfigs = new HashMap<>(configs);
    for (Map.Entry<String, Object> entry: configs.entrySet()) {
      String name = entry.getKey();
      if (name.startsWith(DEFAULT_RULE_PARAM_PREFIX)) {
        ruleConfigs.put(name.substring(DEFAULT_RULE_PARAM_PREFIX.length()), entry.getValue());
      }
    }
    return ruleConfigs;
  }

  public static void main(String[] args) {
    CommandLine commandLine = new CommandLine(new RewrapDeks());
    commandLine.setUsageHelpLongOptionsMaxWidth(30);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }
}