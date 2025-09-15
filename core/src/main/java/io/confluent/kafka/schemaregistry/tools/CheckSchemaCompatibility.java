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

package io.confluent.kafka.schemaregistry.tools;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaProvider;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientFactory;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Optional;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(name = "check-schema-compatibility", mixinStandardHelpOptions = true,
    description = "Compare subjects between two Schema Registry instances and check compatibility. "
               + "Requires --source-url and --target-url to specify the Schema Registry endpoints.",
    sortOptions = false, sortSynopsis = false)
public class CheckSchemaCompatibility implements Callable<Integer> {

  private static final Logger LOG = LoggerFactory.getLogger(CheckSchemaCompatibility.class);

  @Option(names = {"--source-url", "-s"},
      description = "Source Schema Registry URL", paramLabel = "<url>", required = true)
  private String sourceUrl;
  
  @Option(names = {"--target-url", "-t"},
      description = "Target Schema Registry URL", paramLabel = "<url>", required = true)
  private String targetUrl;

  @Option(names = {"--source-context"},
      description = "Context for source Schema Registry (e.g., environment, tenant)",
      paramLabel = "<context>")
  private String sourceContext = "default";

  @Option(names = {"--target-context"},
      description = "Context for target Schema Registry (e.g., environment, tenant)",
      paramLabel = "<context>")
  private String targetContext = "default";

  @Option(names = {"-sc", "--source-credential"},
      description = "Credentials for source Schema Registry authentication "
        + "(username:password)",
      paramLabel = "<username:password>")
  private String sourceCredential;

  @Option(names = {"-dc", "--target-credential"},
      description = "Credentials for destination Schema Registry authentication "
        +  "(username:password)",
      paramLabel = "<username:password>")
  private String targetCredential;

  @Option(names = {"-X", "--property"},
      description = "Set configuration property.", paramLabel = "<prop=val>")
  private Map<String, String> configs;

  public CheckSchemaCompatibility() {

  }

  @Override
  public Integer call() throws Exception {
    try {
      LOG.info("Starting schema compatibility check between:");
      LOG.info("  Source: {} (context: {})", sourceUrl, sourceContext);
      LOG.info("  Target: {} (context: {})", targetUrl, targetContext);

      // Create clients for both registries
      try (SchemaRegistryClient sourceClient = createClient(sourceUrl, sourceCredential);
           SchemaRegistryClient targetClient = createClient(targetUrl, targetCredential)) {
        
        // Get subjects from both registries
        List<String> sourceSubjects = getSubjects(sourceClient, sourceUrl, sourceContext);
        List<String> targetSubjects = getSubjects(targetClient, targetUrl, targetContext);

        // Compare subjects
        if (!compareSubjects(sourceSubjects, targetSubjects, sourceClient, targetClient)) {
          return 1;
        }

        LOG.info("Schema compatibility check completed successfully");
      }
      return 0;
    } catch (Exception e) {
      LOG.error("Error during schema compatibility check: {}", e.getMessage(), e);
      return 1;
    }
  }

  private SchemaRegistryClient createClient(String url, String credential) {
    Map<String, Object> clientConfigs = this.configs != null
        ? new HashMap<>(this.configs)
        : new HashMap<>();
    
    clientConfigs.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, url);
    
    // Add authentication if provided
    if (credential != null && !credential.trim().isEmpty()) {
      clientConfigs.put("basic.auth.credentials.source", "USER_INFO");
      clientConfigs.put("basic.auth.user.info", credential);
    }

    return SchemaRegistryClientFactory.newClient(
        Collections.singletonList(url),
        1000,
        ImmutableList.of(new AvroSchemaProvider()),
        clientConfigs,
        Collections.emptyMap()
    );
  }

  private List<String> getSubjects(SchemaRegistryClient client, String url, String contextName)
      throws IOException, RestClientException {
    Collection<String> subjectsCollection = client.getAllSubjectsByPrefix(contextName);
    List<String> subjects = new ArrayList<>(subjectsCollection);
    LOG.info("Found {} subjects in {} {} context", subjects.size(), url, contextName);
    
    for (String subject : subjects) {
      LOG.info("  {} subject: {}", contextName, subject);
    }
    
    return subjects;
  }

  private boolean compareSubjects(List<String> sourceSubjects, List<String> targetSubjects,
                                  SchemaRegistryClient sourceClient, 
                                  SchemaRegistryClient targetClient) {
    Set<String> sourceSet = new HashSet<>(sourceSubjects);
    Set<String> targetSet = new HashSet<>(targetSubjects);


    // Find subjects only in source
    Set<String> onlyInSource = new HashSet<>(sourceSet);
    onlyInSource.removeAll(targetSet);

    // Find subjects only in target
    Set<String> onlyInTarget = new HashSet<>(targetSet);
    onlyInTarget.removeAll(sourceSet);

    // Find subjects in both
    Set<String> inBoth = new HashSet<>(sourceSet);
    inBoth.retainAll(targetSet);

    // Report results
    LOG.info("\n=== COMPARISON RESULTS ===");
    LOG.info("Total subjects in source ({}): {}", sourceContext, sourceSubjects.size());
    LOG.info("Total subjects in target ({}): {}", targetContext, targetSubjects.size());
    LOG.info("Subjects in both registries: {}", inBoth.size());
    LOG.info("Subjects only in source: {}", onlyInSource.size());
    LOG.info("Subjects only in target: {}", onlyInTarget.size());



    if (!onlyInSource.isEmpty()) {
      LOG.info("\nSubjects only in source ({}):", sourceContext);
      List<String> sortedOnlyInSource = new ArrayList<>(onlyInSource);
      Collections.sort(sortedOnlyInSource);
      for (String subject : sortedOnlyInSource) {
        LOG.info("  - {}", subject);
      }
    }

    if (!onlyInTarget.isEmpty()) {
      LOG.info("\nSubjects only in target ({}):", targetContext);
      List<String> sortedOnlyInTarget = new ArrayList<>(onlyInTarget);
      Collections.sort(sortedOnlyInTarget);
      for (String subject : sortedOnlyInTarget) {
        LOG.info("  - {}", subject);
      }
    }

    if (sourceSet.isEmpty() || targetSet.isEmpty()) {
      LOG.info("\n✓ Compatible, one schema registry is empty");
      return true;
    }

    if (sourceSet.size() != targetSet.size()) {
      LOG.error("\n✓ Not compatible, number of subjects do not match");
      return false;
    }


    if (!inBoth.isEmpty()) {
      LOG.info("\nSubjects in both registries:");
      List<String> sortedInBoth = new ArrayList<>(inBoth);
      Collections.sort(sortedInBoth);
      for (String subject : sortedInBoth) {
        if (!compareSubject(sourceClient, targetClient, subject)) {
          return false;
        }
      }
    }
    return true;
  }

  private boolean compareSubject(SchemaRegistryClient sourceClient, 
                                 SchemaRegistryClient targetClient, 
                                 String subject) {
    try {
      LOG.info("\n--- Comparing subject: {} ---", subject);
      
      // Get all versions for the subject from both registries
      List<Integer> sourceVersions = sourceClient.getAllVersions(subject);
      List<Integer> targetVersions = targetClient.getAllVersions(subject);
      
      LOG.info("Source versions: {}", sourceVersions);
      LOG.info("Target versions: {}", targetVersions);

      // Compare version counts
      if (sourceVersions.size() != targetVersions.size()) {
        LOG.error("✗ Version count mismatch for subject '{}': source={}, target={}",
                 subject, sourceVersions.size(), targetVersions.size());
        return false;
      }
      
      // Sort versions to ensure consistent comparison
      Collections.sort(sourceVersions);
      Collections.sort(targetVersions);
      
      // Compare version numbers
      if (!sourceVersions.equals(targetVersions)) {
        LOG.error("✗ Version numbers don't match for subject '{}': source={}, target={}",
                 subject, sourceVersions, targetVersions);
        return false;
      }
      
      boolean allVersionsMatch = true;
      int mismatchCount = 0;
      
      // Compare each version's schema content and metadata
      for (Integer version : sourceVersions) {
        if (!compareVersion(sourceClient, targetClient, subject, version)) {
          mismatchCount++;
          allVersionsMatch = false;
        }
      }
      
      // Summary for this subject
      if (allVersionsMatch) {
        LOG.info("✓ All {} versions match perfectly for subject '{}'", 
                sourceVersions.size(), subject);
        return true;
      } else {
        LOG.error("✗ Subject '{}' has {} mismatched version(s) out of {}",
                 subject, mismatchCount, sourceVersions.size());
        return false;
      }
      
    } catch (Exception e) {
      LOG.error("✗ Error comparing subject '{}': {}", subject, e.getMessage());
      LOG.error("  Exception details: ", e);
      return false;
    }
  }

  private boolean compareVersion(SchemaRegistryClient sourceClient, 
                                SchemaRegistryClient targetClient, 
                                String subject, 
                                Integer version) {
    try {
      LOG.info("  Comparing version {} for subject '{}'", version, subject);
      
      SchemaMetadata sourceSchema = sourceClient.getSchemaMetadata(subject, version);
      SchemaMetadata targetSchema = targetClient.getSchemaMetadata(subject, version);
      

      // Parse schemas using parseSchema method
      Schema sourceSchemaEntity = new Schema(
          subject,
          version,
          sourceSchema.getId(),
          sourceSchema.getSchemaType(),
          sourceSchema.getReferences(),
          sourceSchema.getMetadata(),
          sourceSchema.getRuleSet(),
          sourceSchema.getSchema()
      );
      
      Schema targetSchemaEntity = new Schema(
          subject,
          version,
          targetSchema.getId(),
          targetSchema.getSchemaType(),
          targetSchema.getReferences(),
          targetSchema.getMetadata(),
          targetSchema.getRuleSet(),
          targetSchema.getSchema()
      );
      
      Optional<ParsedSchema> sourceParsed = sourceClient.parseSchema(sourceSchemaEntity);
      Optional<ParsedSchema> targetParsed = targetClient.parseSchema(targetSchemaEntity);
      
      // Check if both schemas could be parsed
      if (!sourceParsed.isPresent()) {
        LOG.error("✗ Failed to parse source schema for subject '{}' version {}", 
                 subject, version);
        return false;
      } else if (!targetParsed.isPresent()) {
        LOG.error("✗ Failed to parse target schema for subject '{}' version {}", 
                 subject, version);
        return false;
      } else {
        // Use equivalent method for deep comparison
        if (!sourceParsed.get().equivalent(targetParsed.get())) {
          LOG.error("✗ Schema content mismatch for subject '{}' version {}", subject, version);
          LOG.error("  Source schema: {}", sourceSchema.getSchema());
          LOG.error("  Target schema: {}", targetSchema.getSchema());
          return false;
        }
      }
      
      // Compare version number (should match since we're iterating through matched versions)
      if (!version.equals(sourceSchema.getVersion()) 
          || !version.equals(targetSchema.getVersion())) {
        LOG.error("✗ Version number mismatch for subject '{}' version {}: source={}, target={}",
                 subject, version, sourceSchema.getVersion(), targetSchema.getVersion());
        return false;
      }
      
      // Compare schema ID (informational - IDs can differ between registries)
      if (sourceSchema.getId() != targetSchema.getId()) {
        LOG.info("ℹ Schema ID differs for subject '{}' version {}: source={}, target={}",
                subject, version, sourceSchema.getId(), targetSchema.getId());
      }

      LOG.info("✓ Version {} matches for subject '{}'", version, subject);
      return true;
      
    } catch (Exception e) {
      LOG.error("✗ Error comparing version {} for subject '{}': {}",
               version, subject, e.getMessage());
      LOG.error("  Exception details: ", e);
      return false;
    }
  }

  public static void main(String[] args) {
    CommandLine commandLine = new CommandLine(new CheckSchemaCompatibility());
    commandLine.setUsageHelpLongOptionsMaxWidth(30);
    int exitCode = commandLine.execute(args);
    System.exit(exitCode);
  }
}
