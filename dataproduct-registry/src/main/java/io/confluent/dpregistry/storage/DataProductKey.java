/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.dpregistry.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonInclude(Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class DataProductKey implements Comparable<DataProductKey> {

  private final String tenant;
  private final String env;
  private final String cluster;
  private final String name;
  private final int version;

  @JsonCreator
  public DataProductKey(
      @JsonProperty("tenant") String tenant,
      @JsonProperty("env") String env,
      @JsonProperty("cluster") String cluster,
      @JsonProperty("name") String name,
      @JsonProperty("version") int version
  ) {
    this.tenant = tenant;
    this.env = env;
    this.cluster = cluster;
    this.name = name;
    this.version = version;
  }

  @JsonProperty("tenant")
  public String getTenant() {
    return this.tenant;
  }

  @JsonProperty("env")
  public String getEnv() {
    return this.env;
  }

  @JsonProperty("cluster")
  public String getCluster() {
    return this.cluster;
  }

  @JsonProperty("name")
  public String getName() {
    return this.name;
  }

  @JsonProperty("version")
  public int getVersion() {
    return this.version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    DataProductKey that = (DataProductKey) o;
    return Objects.equals(tenant, that.tenant)
        && Objects.equals(env, that.env)
        && Objects.equals(cluster, that.cluster)
        && Objects.equals(name, that.name)
        && version == that.version;
  }

  @Override
  public int hashCode() {
    return Objects.hash(tenant, env, cluster, name, version);
  }

  @Override
  public int compareTo(DataProductKey that) {
    if (this.getTenant() == null && that.getTenant() == null) {
      // pass
    } else if (this.getTenant() == null) {
      return -1;
    } else if (that.getTenant() == null) {
      return 1;
    } else {
      int tenantComparison = this.getTenant().compareTo(that.getTenant());
      if (tenantComparison != 0) {
        return tenantComparison < 0 ? -1 : 1;
      }
    }

    if (this.getEnv() == null && that.getEnv() == null) {
      // pass
    } else if (this.getEnv() == null) {
      return -1;
    } else if (that.getEnv() == null) {
      return 1;
    } else {
      int envComparison = this.getEnv().compareTo(that.getEnv());
      if (envComparison != 0) {
        return envComparison < 0 ? -1 : 1;
      }
    }

    if (this.getCluster() == null && that.getCluster() == null) {
      // pass
    } else if (this.getCluster() == null) {
      return -1;
    } else if (that.getCluster() == null) {
      return 1;
    } else {
      int clusterComparison = this.getCluster().compareTo(that.getCluster());
      if (clusterComparison != 0) {
        return clusterComparison < 0 ? -1 : 1;
      }
    }

    if (this.getName() == null && that.getName() == null) {
      // pass
    } else if (this.getName() == null) {
      return -1;
    } else if (that.getName() == null) {
      return 1;
    } else {
      int nameComparison = this.getName().compareTo(that.getName());
      if (nameComparison != 0) {
        return nameComparison < 0 ? -1 : 1;
      }
    }

    if (this.getVersion() != that.getVersion()) {
      return (this.getVersion() < that.getVersion() ? -1 : 1);
    }

    return 0;
  }
}
