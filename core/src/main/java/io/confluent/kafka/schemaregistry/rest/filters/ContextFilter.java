/*
 * Copyright 2021 Confluent Inc.
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import io.confluent.kafka.schemaregistry.utils.QualifiedSubject;
import io.confluent.rest.entities.ErrorMessage;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.annotation.Priority;
import jakarta.ws.rs.Priorities;
import jakarta.ws.rs.container.ContainerRequestContext;
import jakarta.ws.rs.container.ContainerRequestFilter;
import jakarta.ws.rs.container.PreMatching;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.UriBuilder;
import java.io.IOException;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_PREFIX;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_WILDCARD;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;

@PreMatching
@Priority(Priorities.ENTITY_CODER)
public class ContextFilter implements ContainerRequestFilter {
  private static final Logger log = LoggerFactory.getLogger(ContextFilter.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public ContextFilter() {
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {

    String path = requestContext.getUriInfo().getPath(false);
    if (path.startsWith("contexts/")) {
      try {
        UriBuilder builder = requestContext.getUriInfo().getRequestUriBuilder();
        MultivaluedMap<String, String> queryParams =
            requestContext.getUriInfo().getQueryParameters(false);
        URI uri = modifyUri(builder, path, queryParams);
        requestContext.setRequestUri(uri);
      } catch (IllegalArgumentException e) {
        requestContext.abortWith(
            Response.status(Status.BAD_REQUEST).entity(getErrorResponse(
                Status.BAD_REQUEST, e.getMessage()))
                .build()
        );
      }
    }
  }

  @VisibleForTesting
  URI modifyUri(UriBuilder builder, String path, MultivaluedMap<String, String> queryParams) {
    ContextAndPath contextAndPath = modifyUriPath(path);
    builder.replacePath(contextAndPath.path);
    replaceQueryParams(builder, contextAndPath, queryParams);
    return builder.build();
  }

  /**
   * This method looks for subject in the path param and prefixes the context to the subject in the
   * URI. The subject params are identified as anything after /subjects or /config or /mode based on
   * current Schema Registry resource definition.
   * @param path The original request URI
   * @return The modified request URI
   */
  @VisibleForTesting
  ContextAndPath modifyUriPath(String path) {
    boolean contextPathFound = false;
    String context = DEFAULT_CONTEXT;
    boolean configOrModeFound = false;
    boolean subjectPathFound = false;
    StringBuilder modifiedPath = new StringBuilder();
    boolean isFirst = true;
    for (String uriPathStr : path.split("/")) {

      String modifiedUriPathStr = uriPathStr;

      if (contextPathFound) {
        context = uriPathStr;
        contextPathFound = false;
        continue;
      }

      if (uriPathStr.equals("contexts")) {
        contextPathFound = true;
        continue;
      }

      if (subjectPathFound) {
        if (!startsWithContext(uriPathStr)) {
          modifiedUriPathStr = QualifiedSubject.normalizeContext(context) + uriPathStr;
        }

        subjectPathFound = false;
      }

      boolean isRootConfigOrMode = isRootConfigOrMode(isFirst, uriPathStr);
      if (uriPathStr.equals("subjects")
          || uriPathStr.equals("deks")
          || isRootConfigOrMode) {
        subjectPathFound = true;
        if (isRootConfigOrMode) {
          configOrModeFound = true;
        }
      }

      modifiedPath.append(modifiedUriPathStr).append("/");
      if (isFirst && !uriPathStr.isEmpty()) {
        isFirst = false;
      }
    }
    if (configOrModeFound && subjectPathFound) {
      String normalizedContext = QualifiedSubject.normalizeContext(context);
      if (!normalizedContext.isEmpty()) {
        modifiedPath.append(normalizedContext).append("/");
      }
    } else if (contextPathFound) {
      // Must be a root contexts only
      modifiedPath.append("contexts").append("/");
    } else if ((modifiedPath.isEmpty() || modifiedPath.toString().equals("/"))
        && !DEFAULT_CONTEXT.equals(context)) {
      String normalizedContext = QualifiedSubject.normalizeContext(context);
      modifiedPath.append("contexts").append("/").append(normalizedContext).append("/");
    }

    return new ContextAndPath(context, modifiedPath.toString());
  }

  private boolean isRootConfigOrMode(boolean isFirst, String uriPathStr) {
    return isFirst && (uriPathStr.equals("config") || uriPathStr.equals("mode"));
  }

  private void replaceQueryParams(
      UriBuilder builder,
      ContextAndPath contextAndPath,
      MultivaluedMap<String, String> queryParams) {
    String context = contextAndPath.getContext();
    String path = contextAndPath.getPath();
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
      if (!startsWithContext(subject)) {
        subject = QualifiedSubject.normalizeContext(context) + subject;
        builder.replaceQueryParam("subject", subject);
      }
    } else if (path.equals("schemas") || path.equals("subjects") || path.startsWith("keks")) {
      List<String> subjectPrefixes = queryParams.get("subjectPrefix");
      if (subjectPrefixes == null || subjectPrefixes.isEmpty()) {
        // Ensure context is used as subjectPrefix
        subjectPrefixes = Collections.singletonList("");
      }
      Object[] newSubjectPrefixes = subjectPrefixes.stream()
          .map(prefix -> {
            if (!startsWithContext(prefix)) {
              return QualifiedSubject.normalizeContext(context) + prefix;
            }
            return prefix;
          })
          .toArray();
      builder.replaceQueryParam("subjectPrefix", newSubjectPrefixes);
    }
  }

  private static boolean startsWithContext(String path) {
    try {
      path = URLDecoder.decode(path, "UTF-8");
    } catch (Exception e) {
      // ignore
    }
    return path.startsWith(CONTEXT_PREFIX) || path.startsWith(CONTEXT_WILDCARD);
  }

  public static String getErrorResponse(Response.Status status,
      String message) {
    try {
      ErrorMessage errorMessage = new ErrorMessage(status.getStatusCode(),
          message);
      return MAPPER.writeValueAsString(errorMessage);
    } catch (JsonProcessingException ex) {
      log.error("Could not format response error message. {}", ex.toString());
      return message;
    }
  }

  static class ContextAndPath {
    private final String context;
    private final String path;

    public ContextAndPath(String context, String path) {
      this.context = context;
      this.path = path;
    }

    public String getContext() {
      return context;
    }

    public String getPath() {
      return path;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ContextAndPath that = (ContextAndPath) o;
      return Objects.equals(context, that.context)
          && Objects.equals(path, that.path);
    }

    @Override
    public int hashCode() {
      return Objects.hash(context, path);
    }

    @Override
    public String toString() {
      return "ContextAndPath{"
          + "context='" + context + '\''
          + ", path='" + path + '\''
          + '}';
    }
  }
}
