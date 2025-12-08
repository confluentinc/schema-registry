/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest.filters;

import io.confluent.kafka.schemaregistry.metrics.SchemaRegistryMetric;

import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerResponseContext;
import jakarta.ws.rs.container.ContainerResponseFilter;
import java.io.IOException;

public class RestCallMetricFilter implements ContainerResponseFilter {
  private final SchemaRegistryMetric metricSucceeded;
  private final SchemaRegistryMetric metricFailed;

  public RestCallMetricFilter(SchemaRegistryMetric metricSucceeded,
                              SchemaRegistryMetric metricFailed) {
    this.metricSucceeded = metricSucceeded;
    this.metricFailed = metricFailed;
  }

  @Override
  public void filter(ContainerRequestContext containerRequestContext,
                     ContainerResponseContext containerResponseContext) throws IOException {
    switch (containerResponseContext.getStatusInfo().getFamily()) {
      case SUCCESSFUL:
        metricSucceeded.record();
        break;
      case CLIENT_ERROR:
      case SERVER_ERROR:
        metricFailed.record();
        break;
      default:
        break;
    }
  }
}
