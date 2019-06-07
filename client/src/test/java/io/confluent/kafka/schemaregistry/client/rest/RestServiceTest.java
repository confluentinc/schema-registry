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
import static org.easymock.EasyMock.anyInt;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.anyString;
import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.verify;
import static org.powermock.api.easymock.PowerMock.expectNew;
import static org.powermock.api.easymock.PowerMock.replay;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import avro.shaded.com.google.common.collect.ImmutableMap;
import io.confluent.kafka.schemaregistry.client.security.bearerauth.BearerAuthCredentialProvider;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import io.confluent.kafka.schemaregistry.client.security.basicauth.BasicAuthCredentialProvider;

@RunWith(PowerMockRunner.class)
@PrepareForTest(RestService.class)
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
  
  /*
   * Test setBasicAuthRequestHeader (private method) indirectly through getAllSubjects.
   */
  @Test
  public void testSetBasicAuthRequestHeader() throws Exception {
    RestService restService = new RestService("http://localhost:8081");

    BasicAuthCredentialProvider basicAuthCredentialProvider = createMock(BasicAuthCredentialProvider.class);
    restService.setBasicAuthCredentialProvider(basicAuthCredentialProvider);

    HttpURLConnection httpURLConnection = createNiceMock(HttpURLConnection.class);
    InputStream inputStream = createNiceMock(InputStream.class);

    expectNew(URL.class, anyString()).andReturn(url);
    expect(url.openConnection()).andReturn(httpURLConnection);
    expect(httpURLConnection.getURL()).andReturn(url);
    expect(basicAuthCredentialProvider.getUserInfo(anyObject(URL.class))).andReturn("user:password");
    expect(httpURLConnection.getResponseCode()).andReturn(HttpURLConnection.HTTP_OK);

    // Make sure that the Authorization header is set with the correct value for "user:password"
    httpURLConnection.setRequestProperty("Authorization", "Basic dXNlcjpwYXNzd29yZA==");
    expectLastCall().once();

    expect(httpURLConnection.getInputStream()).andReturn(inputStream);

    expect(inputStream.read((byte[]) anyObject(), anyInt(), anyInt()))
        .andDelegateTo(new InputStream() {
          @Override
          public int read() {
            return 0;
          }

          @Override
          public int read(byte[] b, int off, int len) {
            byte[] json = "[\"abc\"]".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(json, 0, b, 0, json.length);
            return json.length;
          }
        }).anyTimes();

    replay(URL.class, url);
    replay(HttpURLConnection.class, httpURLConnection);
    replay(basicAuthCredentialProvider);
    replay(InputStream.class, inputStream);

    restService.getAllSubjects();

    verify(httpURLConnection);
  }


  /*
   * Test setBasicAuthRequestHeader (private method) indirectly through getAllSubjects.
   */
  @Test
  public void testSetBearerAuthRequestHeader() throws Exception {
    RestService restService = new RestService("http://localhost:8081");

    BearerAuthCredentialProvider bearerAuthCredentialProvider = createMock(BearerAuthCredentialProvider.class);
    restService.setBearerAuthCredentialProvider(bearerAuthCredentialProvider);

    HttpURLConnection httpURLConnection = createNiceMock(HttpURLConnection.class);
    InputStream inputStream = createNiceMock(InputStream.class);

    expectNew(URL.class, anyString()).andReturn(url);
    expect(url.openConnection()).andReturn(httpURLConnection);
    expect(httpURLConnection.getURL()).andReturn(url);
    expect(bearerAuthCredentialProvider.getBearerToken(anyObject(URL.class))).andReturn("auth-token");
    expect(httpURLConnection.getResponseCode()).andReturn(HttpURLConnection.HTTP_OK);

    // Make sure that the Authorization header is set with the correct value for "user:password"
    httpURLConnection.setRequestProperty("Authorization", "Bearer auth-token");
    expectLastCall().once();

    expect(httpURLConnection.getInputStream()).andReturn(inputStream);

    expect(inputStream.read((byte[]) anyObject(), anyInt(), anyInt()))
            .andDelegateTo(new InputStream() {
              @Override
              public int read() {
                return 0;
              }

              @Override
              public int read(byte[] b, int off, int len) {
                byte[] json = "[\"abc\"]".getBytes(StandardCharsets.UTF_8);
                System.arraycopy(json, 0, b, 0, json.length);
                return json.length;
              }
            }).anyTimes();

    replay(URL.class, url);
    replay(HttpURLConnection.class, httpURLConnection);
    replay(bearerAuthCredentialProvider);
    replay(InputStream.class, inputStream);

    restService.getAllSubjects();

    verify(httpURLConnection);
  }
  
  /*
 * Test setHttpHeaders (private method) indirectly through getAllSubjects.
 */
  @Test
  public void testSetHttpHeaders() throws Exception {
    RestService restService = new RestService("http://localhost:8081");

    BasicAuthCredentialProvider basicAuthCredentialProvider = createMock(BasicAuthCredentialProvider.class);
    restService.setHttpHeaders(
        ImmutableMap.of("api-key", "test-api-key","source-app", "foo"));

    HttpURLConnection httpURLConnection = createNiceMock(HttpURLConnection.class);
    InputStream inputStream = createNiceMock(InputStream.class);

    expectNew(URL.class, anyString()).andReturn(url);
    expect(url.openConnection()).andReturn(httpURLConnection);
    expect(httpURLConnection.getResponseCode()).andReturn(HttpURLConnection.HTTP_OK);

    // Make sure that the Authorization header is set with the correct value for "user:password"
    httpURLConnection.setRequestProperty("api-key", "test-api-key");
    httpURLConnection.setRequestProperty("source-app", "foo");
    expectLastCall().once();

    expect(httpURLConnection.getInputStream()).andReturn(inputStream);

    expect(inputStream.read((byte[]) anyObject(), anyInt(), anyInt()))
        .andDelegateTo(new InputStream() {
          @Override
          public int read() {
            return 0;
          }

          @Override
          public int read(byte[] b, int off, int len) {
            byte[] json = "[\"abc\"]".getBytes(StandardCharsets.UTF_8);
            System.arraycopy(json, 0, b, 0, json.length);
            return json.length;
          }
        }).anyTimes();

    replay(URL.class, url);
    replay(HttpURLConnection.class, httpURLConnection);
    replay(InputStream.class, inputStream);

    restService.getAllSubjects();

    verify(httpURLConnection);
  }
}
