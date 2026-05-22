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

package io.confluent.kafka.schemaregistry.encryption;

import java.util.Objects;

/**
 * Per-field outcome of an ENCRYPT rule execution during deserialization.
 *
 * <p>Records appear in a {@code List<EncryptResult>} stored under the
 * {@link EncryptionExecutor#FIELDS_KEY} entry of the corresponding
 * {@code RuleResult.data()} map (accessible via
 * {@code ParsedSchemaAndValue.getRuleResults()}).
 */
public final class EncryptResult {

  /**
   * Outcome of a per-field encrypt rule execution on the read path.
   */
  public enum Status {
    /** Field ciphertext was decrypted to plaintext. */
    DECRYPTED,
    /** Field was left as ciphertext because the executor was configured
     *  for passthrough (non-shared KEK). */
    PASSTHROUGH,
    /** Decryption was attempted but threw. The exception message is in
     *  {@link #errorMessage()}. */
    FAILED
  }

  private final String fieldPath;
  private final Status status;
  private final String kekName;
  private final Integer dekVersion;
  private final String errorMessage;

  public EncryptResult(
      String fieldPath,
      Status status,
      String kekName,
      Integer dekVersion,
      String errorMessage) {
    this.fieldPath = fieldPath;
    this.status = status;
    this.kekName = kekName;
    this.dekVersion = dekVersion;
    this.errorMessage = errorMessage;
  }

  public String fieldPath() {
    return fieldPath;
  }

  public Status status() {
    return status;
  }

  public String kekName() {
    return kekName;
  }

  public Integer dekVersion() {
    return dekVersion;
  }

  public String errorMessage() {
    return errorMessage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof EncryptResult)) {
      return false;
    }
    EncryptResult that = (EncryptResult) o;
    return Objects.equals(fieldPath, that.fieldPath)
        && status == that.status
        && Objects.equals(kekName, that.kekName)
        && Objects.equals(dekVersion, that.dekVersion)
        && Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(fieldPath, status, kekName, dekVersion, errorMessage);
  }

  @Override
  public String toString() {
    return "EncryptResult{fieldPath=" + fieldPath
        + ", status=" + status
        + ", kekName=" + kekName
        + ", dekVersion=" + dekVersion
        + (errorMessage != null ? ", errorMessage=" + errorMessage : "")
        + '}';
  }
}
