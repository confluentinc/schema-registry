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
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SchemaRegistry authorizer using SimpleAclAuthorizer.
 */
public class SchemaRegistrySimpleAclAuthorizer implements RestAuthorizer, RestConfigurable {

  private static final Pattern SUBJECTS_RESOURCE_PATTERN = Pattern.compile("^/subjects/([^/]+)$");
  private static final Pattern SUBJECT_VERSIONS_RESOURCE_PATTERN =
          Pattern.compile("^/subjects/([^/]+)/versions");
  private static final Pattern COMPATIBILITY_RESOURCE_PATTERN =
          Pattern.compile("^/compatibility/subjects/([^/]+)/versions/[^/]+$");
  private static final Pattern CONFIG_RESOURCE_PATTERN = Pattern.compile("^/config/([^/]+)$");
  private static final List<Pattern> SUBJECT_PATTERN_LIST = Arrays.asList(
          SUBJECTS_RESOURCE_PATTERN,
          SUBJECT_VERSIONS_RESOURCE_PATTERN,
          COMPATIBILITY_RESOURCE_PATTERN,
          CONFIG_RESOURCE_PATTERN);

  private static final Logger log =
          LoggerFactory.getLogger(SchemaRegistrySimpleAclAuthorizer.class);

  private Authorizer authorizer = new SimpleAclAuthorizer();

  @Override
  public boolean authorize(String principalName, HttpServletRequest request) {
    if (!authorizationCheckRequired(request)) {
      return true;
    }

    String topicName = getTopicName(request);
    log.debug("Authorizing {} for topic {}", principalName, topicName);
    RequestChannel.Session kafkaSession = getKafkaSession(principalName, request);
    Resource topicResource = Resource.fromString("Topic:" + topicName);
    return authorizer.authorize(kafkaSession, Write$.MODULE$, topicResource);
  }

  private boolean authorizationCheckRequired(HttpServletRequest request) {
    return isModifyingHttpMethod(request) && isProtectedResource(request);
  }

  private boolean isProtectedResource(HttpServletRequest request) {
    return isSubjectsResource(request)
            || isSubjectVersionsResource(request)
            || isCompatibilityResource(request)
            || isConfigResource(request);
  }

  private boolean isModifyingHttpMethod(HttpServletRequest request) {
    return isPost(request) || isDelete(request) || isPut(request);
  }

  private boolean isConfigResource(HttpServletRequest request) {
    return COMPATIBILITY_RESOURCE_PATTERN.matcher(request.getRequestURI()).matches();
  }

  private boolean isCompatibilityResource(HttpServletRequest request) {
    return COMPATIBILITY_RESOURCE_PATTERN.matcher(request.getRequestURI()).matches();
  }

  private boolean isSubjectVersionsResource(HttpServletRequest request) {
    return SUBJECT_VERSIONS_RESOURCE_PATTERN.matcher(request.getRequestURI()).matches();
  }

  private boolean isSubjectsResource(HttpServletRequest request) {
    return SUBJECTS_RESOURCE_PATTERN.matcher(request.getRequestURI()).matches();
  }

  private boolean isPost(HttpServletRequest request) {
    return "POST".equals(request.getMethod());
  }

  private boolean isDelete(HttpServletRequest request) {
    return "DELETE".equals(request.getMethod());
  }

  private boolean isPut(HttpServletRequest request) {
    return "PUT".equals(request.getMethod());
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
    for (Pattern pattern : SUBJECT_PATTERN_LIST) {
      Matcher matcher = pattern.matcher(reqUri);
      if (matcher.find()) {
        subject = matcher.group(1);
      }
    }
    subject = removeSuffix(subject, "-value");
    subject = removeSuffix(subject, "-key");
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
