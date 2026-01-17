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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.kafka.schemaregistry.client.rest.entities.LifecyclePolicy;
import io.confluent.kafka.schemaregistry.client.rest.entities.OpType;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class AssociationCreateOp extends AssociationCreateOrUpdateOp {

  @JsonCreator
  public AssociationCreateOp(
      @JsonProperty("subject") String subject,
      @JsonProperty("associationType") String associationType,
      @JsonProperty("lifecycle") LifecyclePolicy lifecycle,
      @JsonProperty("frozen") Boolean frozen,
      @JsonProperty("schema") RegisterSchemaRequest schema,
      @JsonProperty("normalize") Boolean normalize) {
    super(
        OpType.CREATE,
        subject,
        associationType,
        lifecycle,
        frozen,
        schema,
        normalize);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    return super.equals(o);
  }

  @Override
  public int hashCode() {
    return super.hashCode();
  }
}
