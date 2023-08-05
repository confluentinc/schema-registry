/*
 * Copyright 2023 Confluent Inc.
 */

package io.confluent.dekregistry.client.rest.entities;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.encryption.tink.DekFormat;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.Objects;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class CreateDekRequest {

  private String kmsType;
  private String kmsKeyId;
  private String scope;
  private DekFormat algorithm;
  private String encryptedKeyMaterial;

  public static CreateDekRequest fromJson(String json) throws IOException {
    return JacksonMapper.INSTANCE.readValue(json, CreateDekRequest.class);
  }

  @JsonProperty("kmsType")
  public String getKmsType() {
    return this.kmsType;
  }

  @JsonProperty("kmsType")
  public void setKmsType(String kmsType) {
    this.kmsType = kmsType;
  }

  @JsonProperty("kmsKeyId")
  public String getKmsKeyId() {
    return this.kmsKeyId;
  }

  @JsonProperty("kmsKeyid")
  public void setKmsKeyid(String kmsKeyId) {
    this.kmsKeyId = kmsKeyId;
  }

  @JsonProperty("scope")
  public String getScope() {
    return this.scope;
  }

  @JsonProperty("scope")
  public void setScope(String scope) {
    this.scope = scope;
  }

  @JsonProperty("algorithm")
  public DekFormat getAlgorithm() {
    return this.algorithm;
  }

  @JsonProperty("algorithm")
  public void setAlgorithm(DekFormat algorithm) {
    this.algorithm = algorithm;
  }

  @JsonProperty("encryptedKeyMaterial")
  public String getEncryptedKeyMaterial() {
    return this.encryptedKeyMaterial;
  }

  @JsonProperty("encryptedKeyMaterial")
  public void setEncryptedKeyMaterial(String encryptedKeyMaterial) {
    this.encryptedKeyMaterial = encryptedKeyMaterial;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateDekRequest dek = (CreateDekRequest) o;
    return Objects.equals(kmsType, dek.kmsType)
        && Objects.equals(kmsKeyId, dek.kmsKeyId)
        && Objects.equals(scope, dek.scope)
        && Objects.equals(algorithm, dek.algorithm)
        && Objects.equals(encryptedKeyMaterial, dek.encryptedKeyMaterial);
  }

  @Override
  public int hashCode() {
    return Objects.hash(kmsType, kmsKeyId, scope, algorithm, encryptedKeyMaterial);
  }

  @Override
  public String toString() {
    try {
      return toJson();
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }
}
