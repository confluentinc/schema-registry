/*
 * Copyright 2015 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 *
 */
public class MasterResponse {

  private String masterHost;
  private int masterPort = -1;

  @JsonProperty("master_host")
  public String getMasterHost() {
    return masterHost;
  }

  @JsonProperty("master_host")
  public void setMasterHost(String masterHost) {
    this.masterHost = masterHost;
  }

  @JsonProperty("master_port")
  public int getMasterPort() {
    return masterPort;
  }

  @JsonProperty("master_port")
  public void setMasterPort(int masterPort) {
    this.masterPort = masterPort;
  }

  public static MasterResponse fromJson(String json) throws IOException {
    return new ObjectMapper().readValue(json, MasterResponse.class);
  }
}
