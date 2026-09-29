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
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.InputStream;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClientConfig;
import io.confluent.kafka.schemaregistry.client.rest.utils.UrlList;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;

import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
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
    when(httpURLConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_OK);

    when(httpURLConnection.getInputStream()).thenReturn(inputStream);
    when(inputStream.read(any(), anyInt(), anyInt())).thenAnswer(invocationOnMock -> {
      byte[] b = invocationOnMock.getArgument(0);
      byte[] json = "[\"abc\"]".getBytes(StandardCharsets.UTF_8);
      System.arraycopy(json, 0, b, 0, json.length);
      return json.length;
    });

    restServiceSpy.getAllSubjects();
    // Make sure that the X-Forward header is set to true
    verify(httpURLConnection).setRequestProperty(RestService.X_FORWARD_HEADER, "true");
    verify(httpURLConnection).setRequestProperty(RestService.ACCEPT_UNKNOWN_PROPERTIES, "true");
  }

  /*
   * A transient connection failure (e.g. connect timed out during a rolling restart) is retried
   * when a retry policy is configured, so the request ultimately succeeds.
   */
  @Test
  public void testRetriesTransientConnectFailure() throws Exception {
    RestService restService = new RestService("http://localhost:8081", true);
    restService.setRetries(1, 0, 0); // one retry, no backoff
    RestService restServiceSpy = spy(restService);

    HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);
    InputStream inputStream = mock(InputStream.class);

    doReturn(url).when(restServiceSpy).url(anyString());
    when(url.openConnection()).thenReturn(httpURLConnection);
    // First attempt fails at connect; the retry succeeds.
    when(httpURLConnection.getResponseCode())
        .thenThrow(new SocketTimeoutException("Connect timed out"))
        .thenReturn(HttpURLConnection.HTTP_OK);
    when(httpURLConnection.getInputStream()).thenReturn(inputStream);
    when(inputStream.read(any(), anyInt(), anyInt())).thenAnswer(invocationOnMock -> {
      byte[] b = invocationOnMock.getArgument(0);
      byte[] json = "[\"abc\"]".getBytes(StandardCharsets.UTF_8);
      System.arraycopy(json, 0, b, 0, json.length);
      return json.length;
    });

    assertEquals(Arrays.asList("abc"), restServiceSpy.getAllSubjects());
    // Connect was attempted twice: the initial failure plus one retry.
    verify(httpURLConnection, Mockito.times(2)).getResponseCode();
  }

  /*
   * Without a retry policy (the default for the forwarding constructor), a transient connect
   * failure is not retried and propagates immediately.
   */
  @Test
  public void testNoRetriesFailsOnTransientConnectFailure() throws Exception {
    RestService restService = new RestService("http://localhost:8081", true);
    RestService restServiceSpy = spy(restService);

    HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);

    doReturn(url).when(restServiceSpy).url(anyString());
    when(url.openConnection()).thenReturn(httpURLConnection);
    when(httpURLConnection.getResponseCode())
        .thenThrow(new SocketTimeoutException("Connect timed out"));

    try {
      restServiceSpy.getAllSubjects();
      fail("Expected the request to fail without retries");
    } catch (java.io.IOException expected) {
      // expected
    }
    // Connect was attempted exactly once, i.e. no retry.
    verify(httpURLConnection, Mockito.times(1)).getResponseCode();
  }

  /*
   * With a retry policy configured, an HTTP error response (RestClientException) is NOT retried,
   * even for a status that is retriable by default (503) -- only connection-level IOExceptions are
   * retried on the leader-forwarding path, so a request that reached the leader is not replayed.
   */
  @Test
  public void testRetriesDoNotApplyToHttpErrorResponse() throws Exception {
    RestService restService = new RestService("http://localhost:8081", true);
    restService.setRetries(1, 0, 0); // retries enabled, but must not apply to RestClientException
    RestService restServiceSpy = spy(restService);

    HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);

    doReturn(url).when(restServiceSpy).url(anyString());
    when(url.openConnection()).thenReturn(httpURLConnection);
    when(httpURLConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_UNAVAILABLE);

    try {
      restServiceSpy.getAllSubjects();
      fail("Expected the request to fail without retrying the HTTP error response");
    } catch (RestClientException expected) {
      assertEquals(HttpURLConnection.HTTP_UNAVAILABLE, expected.getStatus());
    }
    // The 503 was returned only once: no retry despite retries being enabled.
    verify(httpURLConnection, Mockito.times(1)).getResponseCode();
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
    verify(httpURLConnection).setRequestProperty(RestService.ACCEPT_UNKNOWN_PROPERTIES, "true");
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
    verify(httpURLConnection).setRequestProperty(RestService.ACCEPT_UNKNOWN_PROPERTIES, "true");
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
    verify(httpURLConnection).setRequestProperty(RestService.ACCEPT_UNKNOWN_PROPERTIES, "true");
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

  @Test
  public void testRandomizeUrls() {
    // test with boolean
    Map<String, Object> configs = new HashMap<>();
    configs.put(SchemaRegistryClientConfig.URL_RANDOMIZE, true);
    UrlList baseUrlSpy = Mockito.spy(new UrlList(Arrays.asList("http://localhost:8080", "http://localhost:8081")));
    RestService restService = new RestService(baseUrlSpy);
    RestService restServiceSpy = spy(restService);
    restServiceSpy.configure(configs);
    verify(baseUrlSpy).randomizeIndex();

    // test with string
    configs.put(SchemaRegistryClientConfig.URL_RANDOMIZE, "true");
    baseUrlSpy = Mockito.spy(new UrlList(Arrays.asList("http://localhost:8080", "http://localhost:8081")));
    restService = new RestService(baseUrlSpy);
    restServiceSpy = spy(restService);
    restServiceSpy.configure(configs);
    verify(baseUrlSpy).randomizeIndex();
  }

  @Test
  public void testExceptionRetry() throws Exception {
    UrlList baseUrlSpy = Mockito.spy(new UrlList(Arrays.asList("http://localhost:8080", "http://localhost:8081")));
    RestService restService = new RestService(baseUrlSpy);
    RestService restServiceSpy = spy(restService);

    HttpURLConnection httpURLConnection = mock(HttpURLConnection.class);

    doReturn(url).when(restServiceSpy).url(anyString());
    when(url.openConnection()).thenReturn(httpURLConnection);
    when(httpURLConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_CLIENT_TIMEOUT);
    try {
      restServiceSpy.getAllSubjects();
      fail("Expected RestClientException to be thrown");
    } catch (RestClientException exception) {
      verify(baseUrlSpy).fail("http://localhost:8080");
      verify(baseUrlSpy).fail("http://localhost:8081");
    }

    // unretryable exception should not be retried
    baseUrlSpy = Mockito.spy(new UrlList(Arrays.asList("http://localhost:8080", "http://localhost:8081")));
    when(httpURLConnection.getResponseCode()).thenReturn(HttpURLConnection.HTTP_BAD_REQUEST);
    try {
      restServiceSpy.getAllSubjects();
      fail("Expected RestClientException to be thrown");
    } catch (RestClientException exception) {
      verify(baseUrlSpy, never()).fail(any());
    }
  }
}
