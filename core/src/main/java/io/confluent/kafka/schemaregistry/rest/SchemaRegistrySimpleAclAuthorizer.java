/**
 * Copyright 2014 Confluent Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rest;

import io.confluent.rest.RestConfig;
import io.confluent.rest.RestConfigurable;
import io.confluent.rest.auth.RestAuthorizer;
import jersey.repackaged.com.google.common.collect.ImmutableMap;
import kafka.network.RequestChannel;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Resource;
import kafka.security.auth.SimpleAclAuthorizer;
import kafka.security.auth.Write$;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.http.HttpServletRequest;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * SchemaRegistry authorizer using SimpleAclAuthorizer.
 */
public class SchemaRegistrySimpleAclAuthorizer implements RestAuthorizer, RestConfigurable {

  private static final Logger log =
          LoggerFactory.getLogger(SchemaRegistrySimpleAclAuthorizer.class);

  private Authorizer authorizer = new SimpleAclAuthorizer();

  @Override
  public boolean authorize(String principalName, HttpServletRequest request) {
    String topicName = getTopicName(request);
    boolean authorized;
    if ("POST".equals(request.getMethod()) && topicName != null) {
      log.debug("Authorizing {} for topic {}", principalName, topicName);
      RequestChannel.Session kafkaSession = getKafkaSession(principalName, request);
      authorized = authorizer
              .authorize(kafkaSession, Write$.MODULE$, Resource.fromString("Topic:" + topicName));
    } else {
      authorized = true;
    }
    return authorized;
  }

  @Override
  public void close() throws Exception {
    authorizer.close();
  }

  @Override
  public void configure(RestConfig config) {
    authorizer.configure(ImmutableMap.of(RestConfig.AUTHORIZATION_ZOOKEEPER_CONNECT_CONFIG,
            config.getString(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG),
            RestConfig.AUTHORIZATION_SUPER_USERS_CONFIG,
            config.getString(RestConfig.AUTHORIZATION_SUPER_USERS_CONFIG)));
  }

  private String getTopicName(HttpServletRequest req) {
    String subject = null;
    String reqUri = req.getRequestURI();
    if (reqUri.startsWith("/subjects")) {
      subject = reqUri.substring("/subjects/".length(), reqUri.indexOf("/versions"));
      subject = removeSuffix(subject, "-value");
      subject = removeSuffix(subject, "-key");
    }
    return subject;
  }

  private RequestChannel.Session getKafkaSession(String principalName, HttpServletRequest req) {
    InetAddress clientAddress = getClientAddress(req);
    log.debug("Request clientAddress {} for principal {}", clientAddress, principalName);
    return new RequestChannel.Session(new KafkaPrincipal("User", principalName), clientAddress);
  }

  private String removeSuffix(String subject, String suffix) {
    if (subject.endsWith(suffix)) {
      subject = subject.substring(0, subject.length() - suffix.length());
    }
    return subject;
  }

  private InetAddress getClientAddress(HttpServletRequest request) {
    String ipAddress = request.getHeader("X-FORWARDED-FOR");
    if (ipAddress == null) {
      ipAddress = request.getRemoteAddr();
    }
    try {
      return InetAddress.getByName(ipAddress);
    } catch (UnknownHostException e) {
      log.error("IpAddress extraction error", e);
    }
    return null;
  }
}
