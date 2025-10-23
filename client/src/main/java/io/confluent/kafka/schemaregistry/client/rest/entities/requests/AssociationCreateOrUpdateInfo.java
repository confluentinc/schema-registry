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

package io.confluent.kafka.schemaregistry.client.rest.entities.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.Objects;

@JsonInclude(JsonInclude.Include.NON_EMPTY)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AssociationCreateOrUpdateInfo {

  private String subject;
  private String associationType;
  private LifecyclePolicy lifecycle;
  private Boolean frozen;
  private Schema schema;
  private Boolean normalize;

  @JsonCreator
  public AssociationCreateOrUpdateInfo(
      @JsonProperty("subject") String subject,
      @JsonProperty("associationType") String associationType,
      @JsonProperty("lifecycle") LifecyclePolicy lifecycle,
      @JsonProperty("frozen") Boolean frozen,
      @JsonProperty("schema") Schema schema,
      @JsonProperty("normalize") Boolean normalize) {
    this.subject = subject;
    this.associationType = associationType;
    this.lifecycle = lifecycle;
    this.frozen = frozen;
    this.schema = schema;
    this.normalize = normalize;
  }

  @JsonProperty("subject")
  public String getSubject() {
    return subject;
  }

  @JsonProperty("subject")
  public void setSubject(String subject) {
    this.subject = subject;
  }

  @JsonProperty("associationType")
  public String getAssociationType() {
    return associationType;
  }

  @JsonProperty("associationType")
  public void setAssociationType(String associationType) {
    this.associationType = associationType;
  }

  @JsonProperty("lifecycle")
  public LifecyclePolicy getLifecycle() {
    return lifecycle;
  }

  @JsonProperty("lifecycle")
  public void setLifecycle(LifecyclePolicy lifecycle) {
    this.lifecycle = lifecycle;
  }

  @JsonProperty("frozen")
  public Boolean getFrozen() {
    return frozen;
  }

  @JsonProperty("frozen")
  public void setFrozen(Boolean frozen) {
    this.frozen = frozen;
  }

  @JsonProperty("schema")
  public Schema getSchema() {
    return schema;
  }

  @JsonProperty("schema")
  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  @JsonProperty("normalize")
  public Boolean getNormalize() {
    return normalize;
  }

  @JsonProperty("normalize")
  public void setNormalize(Boolean normalize) {
    this.normalize = normalize;
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AssociationCreateOrUpdateInfo that = (AssociationCreateOrUpdateInfo) o;
    return Objects.equals(subject, that.subject)
        && Objects.equals(associationType, that.associationType)
        && Objects.equals(lifecycle, that.lifecycle)
        && Objects.equals(frozen, that.frozen)
        && Objects.equals(schema, that.schema)
        && Objects.equals(normalize, that.normalize);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        subject, associationType, lifecycle, frozen, schema, normalize);
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }
}