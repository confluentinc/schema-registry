/**
 * Copyright 2014 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.rest;

import jersey.repackaged.com.google.common.collect.ImmutableMap;
import kafka.security.auth.Acl;
import kafka.security.auth.Resource;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.security.auth.Write$;
import org.eclipse.jetty.servlet.FilterHolder;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.Properties;

import javax.servlet.*;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Configurable;

import io.confluent.kafka.schemaregistry.rest.resources.CompatibilityResource;
import io.confluent.kafka.schemaregistry.rest.resources.ConfigResource;
import io.confluent.kafka.schemaregistry.rest.resources.RootResource;
import io.confluent.kafka.schemaregistry.rest.resources.SchemasResource;
import io.confluent.kafka.schemaregistry.rest.resources.SubjectVersionsResource;
import io.confluent.kafka.schemaregistry.rest.resources.SubjectsResource;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.storage.serialization.SchemaRegistrySerializer;
import io.confluent.rest.Application;
import io.confluent.rest.RestConfigException;
import scala.collection.JavaConversions;
import scala.collection.immutable.Set;

public class SchemaRegistryRestApplication extends Application<SchemaRegistryConfig> {

  private static final Logger log = LoggerFactory.getLogger(SchemaRegistryRestApplication.class);
  private KafkaSchemaRegistry schemaRegistry = null;

  public SchemaRegistryRestApplication(Properties props) throws RestConfigException {
    this(new SchemaRegistryConfig(props));
  }

  public SchemaRegistryRestApplication(SchemaRegistryConfig config) {
    super(config);
  }

  @Override
  public void setupResources(Configurable<?> config, SchemaRegistryConfig schemaRegistryConfig) {
    try {
      schemaRegistry = new KafkaSchemaRegistry(schemaRegistryConfig,
                                               new SchemaRegistrySerializer());
      schemaRegistry.init();
    } catch (SchemaRegistryException e) {
      log.error("Error starting the schema registry", e);
      System.exit(1);
    }
    config.register(RootResource.class);
    config.register(new ConfigResource(schemaRegistry));
    config.register(new SubjectsResource(schemaRegistry));
    config.register(new SchemasResource(schemaRegistry));
    config.register(new SubjectVersionsResource(schemaRegistry));
    config.register(new CompatibilityResource(schemaRegistry));
  }

  protected void configurePreResourceHandling(ServletContextHandler context) {
    Filter filter = new Filter() {
      private final SimpleAclAuthorizer authorizer = new SimpleAclAuthorizer();

      @Override
      public void init(FilterConfig filterConfig) throws ServletException {
        authorizer.configure((ImmutableMap.of("zookeeper.connect", "<zookeeper_connect_url_specify_here>")));
      }

      @Override
      public void doFilter(ServletRequest servletRequest,
                           ServletResponse servletResponse,
                           FilterChain filterChain) throws IOException, ServletException {
        // FIXME -> check authorization only for certain URLs

        // FIXME -> extract topic name from URL
        String topicName = "test";
        Object certsObj = servletRequest.getAttribute("javax.servlet.request.X509Certificate");
        if (certsObj instanceof X509Certificate[]) {
          X509Certificate[] certs = (X509Certificate[]) certsObj;
          boolean canChangeSchema = false;
          for (X509Certificate cert : certs) {
            String user = buildUser(cert);
            Set<Acl> acls = authorizer.getAcls(Resource.fromString("Topic:" + topicName));
            for (Acl acl : JavaConversions.setAsJavaSet(acls)) {
              if (acl.principal().getName().equals(user) && acl.operation().equals(Write$.MODULE$)) {
                canChangeSchema = true;
                break;
              }
            }
          }

          if (!canChangeSchema) {
            String msg = "You are unauthorized to change schema for topic " + topicName;
            if (servletResponse instanceof HttpServletResponse) {
              HttpServletResponse httpResponse = (HttpServletResponse) servletResponse;
              httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, msg);
            } else {
              // it is very unlikely to enter here
              servletResponse.getOutputStream().println(msg);
            }
            return; // do not pass on request processing
          }
        }

        filterChain.doFilter(servletRequest, servletResponse);
      }

      private String buildUser(X509Certificate cert) {
        // FIXME -> allow customizing user name based on certificate
        return "test";
      }

      @Override
      public void destroy() {
        authorizer.close();
      }
    };

    FilterHolder authorizationFilterHolder = new FilterHolder(filter);
    context.addFilter(authorizationFilterHolder, "/*", null);
  }

  @Override
  public void onShutdown() {
    schemaRegistry.close();
  }

  // for testing purpose only
  public KafkaSchemaRegistry schemaRegistry() {
    return schemaRegistry;
  }
}
