/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.kafka.schemaregistry.rest.filters;

import com.google.common.annotations.VisibleForTesting;
import java.net.URI;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Priority;
import javax.ws.rs.Priorities;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.PreMatching;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_DELIMITER;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_PREFIX;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_SEPARATOR;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.CONTEXT_WILDCARD;
import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_CONTEXT;

@PreMatching
@Priority(Priorities.ENTITY_CODER)
public class ContextFilter implements ContainerRequestFilter {
  private static final Logger log = LoggerFactory.getLogger(ContextFilter.class);

  public ContextFilter() {
  }

  @Override
  public void filter(ContainerRequestContext requestContext) throws IOException {

    String path = requestContext.getUriInfo().getPath(false);
    if (path.startsWith("contexts/")) {
      UriBuilder builder = requestContext.getUriInfo().getRequestUriBuilder();
      MultivaluedMap<String, String> queryParams =
          requestContext.getUriInfo().getQueryParameters(false);
      URI uri = modifyUri(builder, path, queryParams);
      requestContext.setRequestUri(uri);
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
        if (!uriPathStr.startsWith(CONTEXT_PREFIX) && !uriPathStr.startsWith(CONTEXT_WILDCARD)) {
          modifiedUriPathStr = formattedContext(context) + uriPathStr;
        }

        subjectPathFound = false;
      }

      boolean isRootConfigOrMode = isRootConfigOrMode(isFirst, uriPathStr);
      if (uriPathStr.equals("subjects") || isRootConfigOrMode) {
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
      String formattedContext = formattedContext(context);
      if (!formattedContext.isEmpty()) {
        modifiedPath.append(formattedContext).append("/");
      }
    } else if (contextPathFound) {
      // Must be a root contexts only
      modifiedPath.append("contexts").append("/");
    }

    return new ContextAndPath(context, modifiedPath.toString());
  }

  private boolean isRootConfigOrMode(boolean isFirst, String uriPathStr) {
    return isFirst && (uriPathStr.equals("config") || uriPathStr.equals("mode"));
  }

  private String formattedContext(String context) {
    if (context.startsWith(CONTEXT_DELIMITER)) {
      context = context.substring(1);
    }
    if (context.endsWith(CONTEXT_DELIMITER)) {
      context = context.substring(0, context.length() - 1);
    }
    if (!context.startsWith(CONTEXT_SEPARATOR)) {
      context = CONTEXT_SEPARATOR + context;
    }
    return DEFAULT_CONTEXT.equals(context) ? "" : CONTEXT_DELIMITER + context + CONTEXT_DELIMITER;
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
      if (!subject.startsWith(CONTEXT_PREFIX) && !subject.startsWith(CONTEXT_WILDCARD)) {
        subject = formattedContext(context) + subject;
        builder.replaceQueryParam("subject", subject);
      }
    } else if (path.equals("schemas") || path.equals("subjects")) {
      String subject = queryParams.getFirst("subjectPrefix");
      if (subject == null) {
        subject = "";
      }
      if (!subject.startsWith(CONTEXT_PREFIX) && !subject.startsWith(CONTEXT_WILDCARD)) {
        subject = formattedContext(context) + subject;
        builder.replaceQueryParam("subjectPrefix", subject);
      }
    }
  }

  static class ContextAndPath {
    private String context;
    private String path;

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
