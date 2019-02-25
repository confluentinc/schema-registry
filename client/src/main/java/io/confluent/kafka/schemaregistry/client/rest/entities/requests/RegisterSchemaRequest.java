/*
 * Copyright 2018 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.PropertyWriter;
import com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;

import java.io.IOException;
import java.util.Objects;

@JsonFilter("registerFilter")
public class RegisterSchemaRequest {

  private int version = 0;
  private int id = -1;
  private String schema;

  public static RegisterSchemaRequest fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, RegisterSchemaRequest.class);
  }

  @JsonProperty("version")
  public int getVersion() {
    return this.version;
  }

  @JsonProperty("version")
  public void setVersion(int version) {
    this.version = version;
  }

  @JsonProperty("id")
  public int getId() {
    return this.id;
  }

  @JsonProperty("id")
  public void setId(int id) {
    this.id = id;
  }

  @JsonProperty("schema")
  public String getSchema() {
    return this.schema;
  }

  @JsonProperty("schema")
  public void setSchema(String schema) {
    this.schema = schema;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    RegisterSchemaRequest that = (RegisterSchemaRequest) o;
    return version == that.version && id == that.id && Objects.equals(schema, that.schema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, id, schema);
  }

  @Override
  public String toString() {
    StringBuilder buf = new StringBuilder();
    buf.append("{");
    if (version > 0) {
      buf.append("version=").append(version).append(", ");
    }
    if (id >= 0) {
      buf.append("id=").append(id).append(", ");
    }
    buf.append("schema=").append(schema).append("}");
    return buf.toString();
  }

  public String toJson() throws IOException {
    FilterProvider filters = new SimpleFilterProvider()
        .addFilter("registerFilter", new RegisterFilter());
    return new ObjectMapper().writer(filters).writeValueAsString(this);
  }

  private static class RegisterFilter extends SimpleBeanPropertyFilter {
    @Override
    public void serializeAsField(
        Object pojo,
        JsonGenerator jgen,
        SerializerProvider provider,
        PropertyWriter writer
    ) throws Exception {
      if (writer.getName().equals("version")) {
        int version = ((RegisterSchemaRequest) pojo).getVersion();
        if (version >= 1) {
          writer.serializeAsField(pojo, jgen, provider);
        }
      } else if (writer.getName().equals("id")) {
        int id = ((RegisterSchemaRequest) pojo).getId();
        if (id >= 0) {
          writer.serializeAsField(pojo, jgen, provider);
        }
      } else {
        writer.serializeAsField(pojo, jgen, provider);
      }
    }
  }

}
