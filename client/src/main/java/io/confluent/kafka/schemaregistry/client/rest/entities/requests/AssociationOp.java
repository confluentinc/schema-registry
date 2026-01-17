/*
 * Copyright 2026 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.client.rest.entities.requests;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import io.confluent.kafka.schemaregistry.client.rest.entities.OpType;
import io.confluent.kafka.schemaregistry.utils.JacksonMapper;
import java.io.IOException;
import java.util.Objects;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = As.EXISTING_PROPERTY,
    property = "opType"
)
@JsonSubTypes({
    @JsonSubTypes.Type(value = AssociationCreateOp.class, name = "CREATE"),
    @JsonSubTypes.Type(value = AssociationUpsertOp.class, name = "UPSERT"),
    @JsonSubTypes.Type(value = AssociationDeleteOp.class, name = "DELETE")
})
public abstract class AssociationOp {

  protected static final String KEY_ASSOCIATION_TYPE = "key";
  protected static final String VALUE_ASSOCIATION_TYPE = "value";

  protected final OpType opType;

  @JsonCreator
  public AssociationOp(
      @JsonProperty("opType") OpType opType
  ) {
    this.opType = opType;
  }

  @JsonProperty("opType")
  public OpType getType() {
    return this.opType;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AssociationOp that = (AssociationOp) o;
    return opType == that.opType;
  }

  @Override
  public int hashCode() {
    return Objects.hash(opType);
  }

  public String toJson() throws IOException {
    return JacksonMapper.INSTANCE.writeValueAsString(this);
  }

  public abstract void validate(boolean dryRun);
}
