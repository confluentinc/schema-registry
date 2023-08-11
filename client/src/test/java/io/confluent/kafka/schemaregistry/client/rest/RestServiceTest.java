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
package io.confluent.kafka.schemaregistry.client.rest;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class RestServiceTest {

  @Test
  public void buildRequestUrl_trimNothing() {
    String baseUrl = "http://test.com";
    String path = "some/path";

    assertEquals("http://test.com/some/path", RestService.buildRequestUrl(baseUrl, path));
  }

  @Test
  public void buildRequestUrl_trimBaseUrl() {
    String baseUrl = "http://test.com/";
    String path = "some/path";

    assertEquals("http://test.com/some/path", RestService.buildRequestUrl(baseUrl, path));
  }

  @Test
  public void buildRequestUrl_trimPath() {
    String baseUrl = "http://test.com";
    String path = "/some/path";

    assertEquals("http://test.com/some/path", RestService.buildRequestUrl(baseUrl, path));
  }

  @Test
  public void buildRequestUrl_trimBaseUrlAndPath() {
    String baseUrl = "http://test.com/";
    String path = "/some/path";

    assertEquals("http://test.com/some/path", RestService.buildRequestUrl(baseUrl, path));
  }

  @Mock
  private URL url;

  @Test
  public void testSetForwardHeader() throws Exception {
    RestService restService = new RestService("http://localhost:8081", true);
    RestService restServiceSpy = spy(restService);

    HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);
    InputStream inputStream = mock(InputStream.class);

    doReturn(url).when(restServiceSpy).url(anyString());
    when(url.openConnection()).thenReturn(httpURLConnection);
    when(httpURLConnection.getURL()).thenReturn(url);
    when(httpURLConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);

    when(httpURLConnection.getInputStream()).thenReturn(inputStream);
    when(inputStream.read(any(), anyInt(), anyInt())).thenAnswer(invocationOnMock -> {
      byte[] b = invocationOnMock.getArgument(0);
      byte[] json = "[\"abc\"]".getBytes(StandardCharsets.UTF_8);
      System.arraycopy(json, 0, b, 0, json.length);
      return json.length;
    });

    Map<String, String> headerProperties = new HashMap<>();
    headerProperties.put("Content-Type", Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED);
    restServiceSpy.getAllSubjects(headerProperties);
    // Make sure that the X-Forward header is set to true
    verify(httpURLConnection).setRequestProperty(RestService.X_FORWARD_HEADER, "true");
  }

  /*
   * Test setBasicAuthRequestHeader (private method) indirectly through getAllSubjects.
   */
  @Test
  public void testSetBasicAuthRequestHeader() throws Exception {
    RestService restService = new RestService("http://localhost:8081");
    RestService restServiceSpy = spy(restService);

    BasicAuthCredentialProvider basicAuthCredentialProvider = mock(BasicAuthCredentialProvider.class);
    restServiceSpy.setBasicAuthCredentialProvider(basicAuthCredentialProvider);

    HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);
    InputStream inputStream = mock(InputStream.class);

    doReturn(url).when(restServiceSpy).url(anyString());
    when(url.openConnection()).thenReturn(httpURLConnection);
    when(httpURLConnection.getURL()).thenReturn(url);
    when(basicAuthCredentialProvider.getUserInfo(any(URL.class))).thenReturn("user:password");
    when(httpURLConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);

    when(httpURLConnection.getInputStream()).thenReturn(inputStream);
    when(inputStream.read(any(), anyInt(), anyInt())).thenAnswer(invocationOnMock -> {
      byte[] b = invocationOnMock.getArgument(0);
      byte[] json = "[\"abc\"]".getBytes(StandardCharsets.UTF_8);
      System.arraycopy(json, 0, b, 0, json.length);
      return json.length;
    });

    restServiceSpy.getAllSubjects();
    // Make sure that the Authorization header is set with the correct value for "user:password"
    verify(httpURLConnection).setRequestProperty("Authorization", "Basic dXNlcjpwYXNzd29yZA==");
  }


  /*
   * Test setBearerAuthRequestHeader (private method) indirectly through getAllSubjects.
   */
  @Test
  public void testSetBearerAuthRequestHeader() throws Exception {
    RestService restService = new RestService("http://localhost:8081");
    RestService restServiceSpy = spy(restService);

    BearerAuthCredentialProvider bearerAuthCredentialProvider = mock(BearerAuthCredentialProvider.class);
    restServiceSpy.setBearerAuthCredentialProvider(bearerAuthCredentialProvider);

    HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);
    InputStream inputStream = mock(InputStream.class);

    doReturn(url).when(restServiceSpy).url(anyString());
    when(url.openConnection()).thenReturn(httpURLConnection);
    when(httpURLConnection.getURL()).thenReturn(url);
    when(bearerAuthCredentialProvider.getBearerToken(any(URL.class))).thenReturn("auth-token");
    when(bearerAuthCredentialProvider.getTargetSchemaRegistry()).thenReturn("lsrc-dummy");
    when(bearerAuthCredentialProvider.getTargetIdentityPoolId()).thenReturn("my-pool-id");
    when(httpURLConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);

    when(httpURLConnection.getInputStream()).thenReturn(inputStream);
    when(inputStream.read(any(), anyInt(), anyInt())).thenAnswer(invocationOnMock -> {
      byte[] b = invocationOnMock.getArgument(0);
      byte[] json = "[\"abc\"]".getBytes(StandardCharsets.UTF_8);
      System.arraycopy(json, 0, b, 0, json.length);
      return json.length;
    });

    restServiceSpy.getAllSubjects();


    // Make sure that the Authorization header is set with the correct token
    verify(httpURLConnection).setRequestProperty("Authorization", "Bearer auth-token");
    verify(httpURLConnection).setRequestProperty("target-sr-cluster", "lsrc-dummy");
    verify(httpURLConnection).setRequestProperty("Confluent-Identity-Pool-Id", "my-pool-id");
  }

  /*
 * Test setHttpHeaders (private method) indirectly through getAllSubjects.
 */
  @Test
  public void testSetHttpHeaders() throws Exception {
    RestService restService = new RestService("http://localhost:8081");
    RestService restServiceSpy = spy(restService);

    restServiceSpy.setHttpHeaders(
        ImmutableMap.of("api-key", "test-api-key","source-app", "foo"));

    HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);
    InputStream inputStream = mock(InputStream.class);

    doReturn(url).when(restServiceSpy).url(anyString());
    when(url.openConnection()).thenReturn(httpURLConnection);
    when(httpURLConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);

    when(httpURLConnection.getInputStream()).thenReturn(inputStream);
    when(inputStream.read(any(), anyInt(), anyInt())).thenAnswer(invocationOnMock -> {
      byte[] b = invocationOnMock.getArgument(0);
      byte[] json = "[\"abc\"]".getBytes(StandardCharsets.UTF_8);
      System.arraycopy(json, 0, b, 0, json.length);
      return json.length;
    });

    restServiceSpy.getAllSubjects();

    // Make sure that the correct header is set
    verify(httpURLConnection).setRequestProperty("api-key", "test-api-key");
    verify(httpURLConnection).setRequestProperty("source-app", "foo");
  }

  @Test
  public void testErrorResponseWithNullErrorStreamFromConnection() throws Exception {
    RestService restService = new RestService("http://localhost:8081");
    RestService restServiceSpy = spy(restService);

    HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);

    doReturn(url).when(restServiceSpy).url(anyString());
    when(url.openConnection()).thenReturn(httpURLConnection);
    when(httpURLConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
    when(httpURLConnection.getErrorStream()).thenReturn(null);

    try {
      restServiceSpy.getAllSubjects();
      fail("Expected RestClientException to be thrown");
    } catch (RestClientException exception) {
      assertTrue(exception.getMessage().endsWith("error code: 50005"));
    }
  }

  @Test
  public void testSetProxy() throws Exception {
    RestService restService = new RestService("http://localhost:8081");
    RestService restServiceSpy = spy(restService);

    HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);
    InputStream inputStream = mock(InputStream.class);
    Map<String, Object> configs = new HashMap<>();
    configs.put("proxy.host", "http://localhost");
    configs.put("proxy.port", 8080);
    restServiceSpy.configure(configs);

    doReturn(url).when(restServiceSpy).url(anyString());

    when(httpURLConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);
    when(url.openConnection(any())).thenReturn(httpURLConnection);
    when(httpURLConnection.getInputStream()).thenReturn(inputStream);
    when(inputStream.read(any(), anyInt(), anyInt())).thenAnswer(invocationOnMock -> {
      byte[] b = invocationOnMock.getArgument(0);
      byte[] json = "[\"abc\"]".getBytes(StandardCharsets.UTF_8);
      System.arraycopy(json, 0, b, 0, json.length);
      return json.length;
    });

    restServiceSpy.getAllSubjects();

    ArgumentCaptor<Proxy> proxyCaptor = ArgumentCaptor.forClass(Proxy.class);
    verify(url).openConnection(proxyCaptor.capture());
    InetSocketAddress inetAddress = (InetSocketAddress) proxyCaptor.getValue().address();
    assertEquals("http://localhost", inetAddress.getHostName());
    assertEquals(8080, inetAddress.getPort());
  }
}
