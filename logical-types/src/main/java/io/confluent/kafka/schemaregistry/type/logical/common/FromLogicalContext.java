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

package io.confluent.kafka.schemaregistry.type.logical.common;

import com.google.protobuf.Descriptors.FileDescriptor;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaReference;
import io.confluent.kafka.schemaregistry.type.logical.LogicalType;
import io.confluent.kafka.schemaregistry.type.logical.Schema;
import io.confluent.kafka.schemaregistry.type.logical.ValidationException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Mutable context for converting logical type {@link Schema} to a format-specific schema.
 * Wraps the input {@link LogicalType} and accumulates conversion-time state
 * (converted named types cache, external file descriptor dependencies for Protobuf).
 *
 * @param <T> the format-specific schema type (e.g., org.apache.avro.Schema)
 */
public final class FromLogicalContext<T> {

  private final LogicalType logicalType;
  private final LogicalTypeVersion version;
  private final Map<String, T> convertedNamedTypes = new LinkedHashMap<>();
  private final Set<String> inProgressNamedTypes = new HashSet<>();
  private final List<FileDescriptor> externalDependencies = new ArrayList<>();
  // FQN → import-name to use when a field references this external. Set by
  // the proto writer's registerExternalType. The import name is the source
  // SchemaReference name (the resolvedReferences key), so consumers can
  // resolve emitted `import "X";` statements against the references list.
  // For files containing multiple types, every FQN maps to the FIRST source
  // ref's rename so the proto compiler doesn't see duplicate symbol
  // definitions across multiple renamed clones of the same file.
  private final Map<String, String> externalImportNameByFqn = new LinkedHashMap<>();
  // Source-ref-name → already-emitted rename, so a file with N contained
  // types only contributes ONE renamed clone to externalDependencies.
  private final Map<String, String> externalFileRenamedTo = new LinkedHashMap<>();

  public FromLogicalContext(LogicalType logicalType) {
    this(logicalType, LogicalTypeVersion.V2);
  }

  public FromLogicalContext(LogicalType logicalType, LogicalTypeVersion version) {
    this.logicalType = logicalType;
    this.version = version;
  }

  public LogicalType getLogicalType() {
    return logicalType;
  }

  public LogicalTypeVersion getVersion() {
    return version;
  }

  public boolean isV1() {
    return version == LogicalTypeVersion.V1;
  }

  public Map<String, Schema> getNamedTypes() {
    return logicalType.getNamedTypes();
  }

  public List<SchemaReference> getReferences() {
    return logicalType.getReferences();
  }

  public Map<String, String> getResolvedReferences() {
    return logicalType.getResolvedReferences();
  }

  public boolean hasNamedType(String name) {
    return logicalType.getNamedTypes().containsKey(name);
  }

  public Schema getNamedType(String name) {
    return logicalType.getNamedTypes().get(name);
  }

  public Map<String, T> getConvertedNamedTypes() {
    return convertedNamedTypes;
  }

  public boolean isAlreadyConverted(String name) {
    return convertedNamedTypes.containsKey(name);
  }

  public T getConverted(String name) {
    return convertedNamedTypes.get(name);
  }

  public void putConverted(String name, T converted) {
    if (convertedNamedTypes.containsKey(name)) {
      throw new ValidationException("Duplicate named type: " + name);
    }
    convertedNamedTypes.put(name, converted);
  }

  public boolean isInProgress(String name) {
    return inProgressNamedTypes.contains(name);
  }

  public void markInProgress(String name) {
    inProgressNamedTypes.add(name);
  }

  public void unmarkInProgress(String name) {
    inProgressNamedTypes.remove(name);
  }

  public List<FileDescriptor> getExternalDependencies() {
    return externalDependencies;
  }

  public void addExternalDependency(FileDescriptor dependency) {
    externalDependencies.add(dependency);
  }

  /**
   * Return the import name to use for {@code fqn}, or {@code null} if the
   * FQN was not registered as an external (callers fall back to the FQN).
   * For the proto writer, this is the SR ref name used as the
   * {@code import "..."} string in the emitted file.
   */
  public String externalImportNameFor(String fqn) {
    return externalImportNameByFqn.get(fqn);
  }

  public void registerExternalImport(String fqn, String importName) {
    externalImportNameByFqn.put(fqn, importName);
  }

  public String externalFileAlreadyRenamedTo(String sourceFileName) {
    return externalFileRenamedTo.get(sourceFileName);
  }

  public void markExternalFileRenamed(String sourceFileName, String renameTo) {
    externalFileRenamedTo.put(sourceFileName, renameTo);
  }
}
