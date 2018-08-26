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

package io.confluent.kafka.schemaregistry.rest.extensions;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.jaxrs.cfg.EndpointConfigBase;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterInjector;
import com.fasterxml.jackson.jaxrs.cfg.ObjectWriterModifier;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlParser;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Configurable;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriInfo;
import java.io.IOException;
import java.net.MalformedURLException;

/**
 * A {@link SchemaRegistryResourceExtension} that intercepts all requests and injects an
 * {@link com.fasterxml.jackson.databind.ObjectWriter} to pretty-print any returned objects using
 * Jackson's {@link com.fasterxml.jackson.databind.ObjectMapper}.
 * <p></p>
 * Adding a boolean query parameter of {@code ?pretty} to the URL will enable the
 * {@link SerializationFeature#INDENT_OUTPUT} feature.
 */
public class PrettyQueryExtension implements SchemaRegistryResourceExtension {

  private static final Logger log =
          LoggerFactory.getLogger(PrettyQueryExtension.class);

  public static final String QUERY_PARAM_PRETTY = "pretty";

  @Override
  public void register(
          Configurable<?> config,
          SchemaRegistryConfig schemaRegistryConfig,
          SchemaRegistry schemaRegistry
  ) throws SchemaRegistryException {
    log.trace("Configuring Responses to accept ?{}=true QueryParams", QUERY_PARAM_PRETTY);
    config.register(new ContainerResponseFilter() {
      @Override
      public void filter(
              ContainerRequestContext requestContext,
              ContainerResponseContext responseContext
      ) throws IOException {
        if (hasPrettyQueryParam(requestContext.getUriInfo())) {
          log.trace("Injecting Indenting ObjectWriter");
          ObjectWriterInjector.set(new IndentingModifier(true));
        }
      }
    });
  }

  public static boolean hasPrettyQueryParam(UriInfo uriInfo) {
    try {
      return new UrlParser(uriInfo.getRequestUri().toURL())
              .hasBooleanQueryParam(QUERY_PARAM_PRETTY);
    } catch (MalformedURLException e) {
      log.error("Malformed URL", e);
      return false;
    }
  }

  @Override
  public void close() throws IOException {

  }

  public static class IndentingModifier extends ObjectWriterModifier {

    private final boolean indent;

    public IndentingModifier(boolean indent) {
      this.indent = indent;
    }

    @Override
    public ObjectWriter modify(
            EndpointConfigBase<?> endpointConfigBase,
            MultivaluedMap<String, Object> multivaluedMap,
            Object o,
            ObjectWriter objectWriter,
            JsonGenerator jsonGenerator
    ) throws IOException {
      if (indent) {
        return objectWriter.withFeatures(SerializationFeature.INDENT_OUTPUT);
      }
      return objectWriter;
    }
  }
}
