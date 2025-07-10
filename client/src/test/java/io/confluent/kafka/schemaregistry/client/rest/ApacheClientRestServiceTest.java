/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.rest;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.net.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.lang.reflect.Field;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;

import com.google.common.collect.ImmutableMap;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.routing.DefaultProxyRoutePlanner;
import org.junit.Test;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;

@RunWith(MockitoJUnitRunner.class)
public class ApacheClientRestServiceTest {

  @Test
  public void testSetForwardHeader() throws Exception {
    RestService restService = new RestService("http://localhost:8081", true, true);
    RestService restServiceSpy = spy(restService);

    CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity entity = mock(HttpEntity.class);

    when(httpClient.executeOpen(any(), any(), any())).thenReturn(response);
    when(response.getCode()).thenReturn(200);
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream("[\"abc\"]".getBytes()));

    // Use reflection to set the httpClient field
    Field httpClientField = RestService.class.getDeclaredField("httpClient");
    httpClientField.setAccessible(true);
    httpClientField.set(restServiceSpy, httpClient);

    Map<String, String> headerProperties = new HashMap<>();
    headerProperties.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    restServiceSpy.getAllSubjects(headerProperties);

    // Verify the request was made with the forward header
    ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);
    verify(httpClient).executeOpen(any(), requestCaptor.capture(), any());
    HttpGet capturedRequest = requestCaptor.getValue();
    assertEquals("true", capturedRequest.getHeader(RestService.X_FORWARD_HEADER).getValue());
  }

  /*
   * Test setBasicAuthRequestHeader (private method) indirectly through getAllSubjects.
   */
  @Test
  public void testSetBasicAuthRequestHeader() throws Exception {
    RestService restService = new RestService("http://localhost:8081", false, true);
    RestService restServiceSpy = spy(restService);

    BasicAuthCredentialProvider basicAuthCredentialProvider = mock(BasicAuthCredentialProvider.class);
    restServiceSpy.setBasicAuthCredentialProvider(basicAuthCredentialProvider);

    CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity entity = mock(HttpEntity.class);

    when(httpClient.executeOpen(any(), any(), any())).thenReturn(response);
    when(response.getCode()).thenReturn(200);
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream("[\"abc\"]".getBytes()));

    // Use reflection to set the httpClient field
    Field httpClientField = RestService.class.getDeclaredField("httpClient");
    httpClientField.setAccessible(true);
    httpClientField.set(restServiceSpy, httpClient);

    when(basicAuthCredentialProvider.getUserInfo(any(URL.class))).thenReturn("user:password");

    restServiceSpy.getAllSubjects();

    // Verify the request was made with the correct Authorization header
    ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);
    verify(httpClient).executeOpen(any(), requestCaptor.capture(), any());
    HttpGet capturedRequest = requestCaptor.getValue();
    assertEquals("Basic dXNlcjpwYXNzd29yZA==", capturedRequest.getHeader("Authorization").getValue());
  }

  /*
   * Test setBearerAuthRequestHeader (private method) indirectly through getAllSubjects.
   */
  @Test
  public void testSetBearerAuthRequestHeader() throws Exception {
    RestService restService = new RestService("http://localhost:8081", false, true);
    RestService restServiceSpy = spy(restService);

    BearerAuthCredentialProvider bearerAuthCredentialProvider = mock(BearerAuthCredentialProvider.class);
    restServiceSpy.setBearerAuthCredentialProvider(bearerAuthCredentialProvider);

    CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity entity = mock(HttpEntity.class);

    when(httpClient.executeOpen(any(), any(), any())).thenReturn(response);
    when(response.getCode()).thenReturn(200);
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream("[\"abc\"]".getBytes()));

    // Use reflection to set the httpClient field
    Field httpClientField = RestService.class.getDeclaredField("httpClient");
    httpClientField.setAccessible(true);
    httpClientField.set(restServiceSpy, httpClient);

    when(bearerAuthCredentialProvider.getBearerToken(any(URL.class))).thenReturn("auth-token");
    when(bearerAuthCredentialProvider.getTargetSchemaRegistry()).thenReturn("lsrc-dummy");
    when(bearerAuthCredentialProvider.getTargetIdentityPoolId()).thenReturn("my-pool-id");

    restServiceSpy.getAllSubjects();

    // Verify the request was made with the correct headers
    ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);
    verify(httpClient).executeOpen(any(), requestCaptor.capture(), any());
    HttpGet capturedRequest = requestCaptor.getValue();
    assertEquals("Bearer auth-token", capturedRequest.getHeader("Authorization").getValue());
    assertEquals("lsrc-dummy", capturedRequest.getHeader("target-sr-cluster").getValue());
    assertEquals("my-pool-id", capturedRequest.getHeader("Confluent-Identity-Pool-Id").getValue());
  }

  /*
   * Test setHttpHeaders (private method) indirectly through getAllSubjects.
   */
  @Test
  public void testSetHttpHeaders() throws Exception {
    RestService restService = new RestService("http://localhost:8081", false, true);
    RestService restServiceSpy = spy(restService);

    CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity entity = mock(HttpEntity.class);

    when(httpClient.executeOpen(any(), any(), any())).thenReturn(response);
    when(response.getCode()).thenReturn(200);
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream("[\"abc\"]".getBytes()));

    Field httpClientField = RestService.class.getDeclaredField("httpClient");
    httpClientField.setAccessible(true);
    httpClientField.set(restServiceSpy, httpClient);

    restServiceSpy.setHttpHeaders(
        ImmutableMap.of("api-key", "test-api-key", "source-app", "foo"));
    restServiceSpy.getAllSubjects();

    // Make sure that the correct header is set
    ArgumentCaptor<HttpGet> requestCaptor = ArgumentCaptor.forClass(HttpGet.class);
    verify(httpClient).executeOpen(any(), requestCaptor.capture(), any());
    HttpGet capturedRequest = requestCaptor.getValue();
    assertEquals("test-api-key", capturedRequest.getHeader("api-key").getValue());
    assertEquals("foo", capturedRequest.getHeader("source-app").getValue());
  }

  @Test
  public void testErrorResponseWithNullErrorStreamFromConnection() throws Exception {
    RestService restService = new RestService("http://localhost:8081", false, true);
    RestService restServiceSpy = spy(restService);

    CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity entity = mock(HttpEntity.class);

    when(httpClient.executeOpen(any(), any(), any())).thenReturn(response);
    when(response.getCode()).thenReturn(400);
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream("Error".getBytes()));

    Field httpClientField = RestService.class.getDeclaredField("httpClient");
    httpClientField.setAccessible(true);
    httpClientField.set(restServiceSpy, httpClient);

    try {
      restServiceSpy.getAllSubjects();
      fail("Expected RestClientException to be thrown");
    } catch (RestClientException exception) {
      assertTrue(exception.getMessage().endsWith("error code: 50005"));
    }
  }

  @Test
  public void testSetProxy() throws Exception {
    RestService restService = new RestService("http://localhost:8081", false, true);

    Map<String, Object> configs = new HashMap<>();
    configs.put("proxy.host", "http://localhost");
    configs.put("proxy.port", 8082);
    configs.put("use.apache.http.client", "true");
    restService.configure(configs);
    restService.getApacheHttpClient();

    Field httpClientField = RestService.class.getDeclaredField("httpClient");
    httpClientField.setAccessible(true);
    CloseableHttpClient httpClient = (CloseableHttpClient) httpClientField.get(restService);
    Class<?> implClass = httpClient.getClass();

    Field plannerField = implClass.getDeclaredField("routePlanner");
    plannerField.setAccessible(true);
    Object rawPlanner = plannerField.get(httpClient);

    if (!(rawPlanner instanceof DefaultProxyRoutePlanner)) {
      throw new AssertionError("Expected DefaultProxyRoutePlanner, got " + rawPlanner.getClass());
    }
    DefaultProxyRoutePlanner proxyPlanner = (DefaultProxyRoutePlanner) rawPlanner;

    Field proxyField = proxyPlanner.getClass()
        .getDeclaredField("proxy");
    proxyField.setAccessible(true);
    HttpHost proxy = (HttpHost) proxyField.get(proxyPlanner);

    assertEquals("localhost", proxy.getHostName());
    assertEquals(8082, proxy.getPort());
  }

  @Test
  public void testRandomizeUrls() {
    // test with boolean
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.URL_RANDOMIZE, true);
    UrlList baseUrlSpy = Mockito.spy(new UrlList(Arrays.asList("http://localhost:8080", "http://localhost:8081")));
    RestService restService = new RestService(baseUrlSpy, false, true);
    RestService restServiceSpy = spy(restService);
    restServiceSpy.configure(configs);
    verify(baseUrlSpy).randomizeIndex();

    // test with string
    configs.put(SchemaRegistryClientConfig.URL_RANDOMIZE, "true");
    baseUrlSpy = Mockito.spy(new UrlList(Arrays.asList("http://localhost:8080", "http://localhost:8081")));
    restService = new RestService(baseUrlSpy, false, true);
    restServiceSpy = spy(restService);
    restServiceSpy.configure(configs);
    verify(baseUrlSpy).randomizeIndex();
  }

  @Test
  public void testExceptionRetry() throws Exception {
    UrlList baseUrlSpy = Mockito.spy(new UrlList(Arrays.asList("http://localhost:8080", "http://localhost:8081")));
    RestService restService = new RestService(baseUrlSpy, false, true);
    RestService restServiceSpy = spy(restService);

    CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    HttpEntity entity = mock(HttpEntity.class);

    when(httpClient.executeOpen(any(), any(), any())).thenReturn(response);
    when(response.getCode()).thenReturn(408); // HTTP_CLIENT_TIMEOUT
    when(response.getEntity()).thenReturn(entity);
    when(entity.getContent()).thenReturn(new ByteArrayInputStream("Error".getBytes()));

    // Use reflection to set the httpClient field
    Field httpClientField = RestService.class.getDeclaredField("httpClient");
    httpClientField.setAccessible(true);
    httpClientField.set(restServiceSpy, httpClient);

    try {
      restServiceSpy.getAllSubjects();
      fail("Expected RestClientException to be thrown");
    } catch (RestClientException exception) {
      verify(baseUrlSpy).fail("http://localhost:8080");
      verify(baseUrlSpy).fail("http://localhost:8081");
    }

    // unretryable exception should not be retried
    baseUrlSpy = Mockito.spy(new UrlList(Arrays.asList("http://localhost:8080", "http://localhost:8081")));
    when(response.getCode()).thenReturn(400); // HTTP_BAD_REQUEST
    try {
      restServiceSpy.getAllSubjects();
      fail("Expected RestClientException to be thrown");
    } catch (RestClientException exception) {
      verify(baseUrlSpy, never()).fail(any());
    }
  }
}
