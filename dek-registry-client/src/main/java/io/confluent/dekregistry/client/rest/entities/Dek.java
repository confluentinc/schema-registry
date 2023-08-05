/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import java.util.Objects;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class Dek {

  private final String kekName;
  private final String scope;
  private final DekFormat algorithm;
  private final String encryptedKeyMaterial;
  private final String keyMaterial;

  @JsonCreator
  public Dek(
      @JsonProperty("kekName") String kekName,
      @JsonProperty("scope") String scope,
      @JsonProperty("algorithm") DekFormat algorithm,
      @JsonProperty("encryptedKeyMaterial") String encryptedKeyMaterial,
      @JsonProperty("keyMaterial") String keyMaterial
  ) {
    this.kekName = kekName;
    this.scope = scope;
    this.algorithm = algorithm;
    this.encryptedKeyMaterial = encryptedKeyMaterial;
    this.keyMaterial = keyMaterial;
  }

  @JsonProperty("kekName")
  public String getKekName() {
    return this.kekName;
  }

  @JsonProperty("scope")
  public String getScope() {
    return this.scope;
  }

  @JsonProperty("algorithm")
  public DekFormat getAlgorithm() {
    return this.algorithm;
  }

  @JsonProperty("encryptedKeyMaterial")
  public String getEncryptedKeyMaterial() {
    return this.encryptedKeyMaterial;
  }

  @JsonProperty("keyMaterial")
  public String getKeyMaterial() {
    return this.keyMaterial;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Dek dek = (Dek) o;
    return Objects.equals(kekName, dek.kekName)
        && Objects.equals(scope, dek.scope)
        && Objects.equals(algorithm, dek.algorithm)
        && Objects.equals(encryptedKeyMaterial, dek.encryptedKeyMaterial)
        && Objects.equals(keyMaterial, dek.keyMaterial);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kekName, scope, algorithm, encryptedKeyMaterial, keyMaterial);
  }
}
