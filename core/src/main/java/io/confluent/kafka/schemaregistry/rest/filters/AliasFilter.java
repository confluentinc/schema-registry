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

package io.confluent.kafka.schemaregistry.rest.filters;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import java.io.IOException;
import java.net.URI;
import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@PreMatching
@Priority(Priorities.ENTITY_CODER + 100) // ensure runs after ContextFilter
public class AliasFilter implements ContainerRequestFilter {
  private static final Logger log = LoggerFactory.getLogger(AliasFilter.class);

  private final KafkaSchemaRegistry schemaRegistry;

  public AliasFilter(KafkaSchemaRegistry schemaRegistry) {
    this.schemaRegistry = schemaRegistry;
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {

    String path = requestContext.getUriInfo().getPath(false);
    UriBuilder builder = requestContext.getUriInfo().getRequestUriBuilder();
    MultivaluedMap<String, String> queryParams =
        requestContext.getUriInfo().getQueryParameters(false);
    URI uri = modifyUri(builder, path, queryParams);
    requestContext.setRequestUri(uri);
  }

  @VisibleForTesting
  URI modifyUri(UriBuilder builder, String path, MultivaluedMap<String, String> queryParams) {
    String modifiedPath = modifyUriPath(path);
    builder.replacePath(modifiedPath);
    replaceQueryParams(builder, modifiedPath, queryParams);
    return builder.build();
  }

  /**
   * This method looks for subject in the path param and and checks if it is an alias.
   * If so, it replaces the alias with the actual subject.
   * @param path The original request URI
   * @return The modified request URI
   */
  @VisibleForTesting
  String modifyUriPath(String path) {
    boolean subjectPathFound = false;
    StringBuilder modifiedPath = new StringBuilder();
    boolean isFirst = true;
    for (String uriPathStr : path.split("/")) {

      String modifiedUriPathStr = uriPathStr;

      if (subjectPathFound) {
        modifiedUriPathStr = replaceAlias(uriPathStr);
        subjectPathFound = false;
      }

      if (uriPathStr.equals("subjects")) {
        subjectPathFound = true;
      }

      modifiedPath.append(modifiedUriPathStr).append("/");
      if (isFirst && !uriPathStr.isEmpty()) {
        isFirst = false;
      }
    }

    return modifiedPath.toString();
  }

  private void replaceQueryParams(
      UriBuilder builder,
      String path,
      MultivaluedMap<String, String> queryParams) {
    if (path.endsWith("/")) {
      path = path.substring(0, path.length() - 1);
    }
    if (path.startsWith("/")) {
      path = path.substring(1);
    }

    if (path.startsWith("schemas/ids")) {
      String subject = queryParams.getFirst("subject");
      if (subject == null) {
        subject = "";
      }
      builder.replaceQueryParam("subject", replaceAlias(subject));
    }
  }

  private String replaceAlias(String subject) {
    if (subject.isEmpty()) {
      return subject;
    }
    Config config = null;
    try {
      config = schemaRegistry.getConfig(subject);
    } catch (Exception e) {
      // fall through
    }
    if (config == null) {
      return subject;
    }
    String alias = config.getAlias();
    return alias != null && !alias.isEmpty() ? alias : subject;
  }
}
