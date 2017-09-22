/**
 * Copyright 2015 Confluent Inc.
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
import java.util.List;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

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
    RestService restService = new RestService("http://user:password@localhost:8081");

    HttpURLConnection httpURLConnection = createNiceMock(HttpURLConnection.class);
    InputStream inputStream = createNiceMock(InputStream.class);
    
    expectNew(URL.class, anyString()).andReturn(url).anyTimes();

    expect(url.openConnection()).andReturn(httpURLConnection).anyTimes();

    expect(url.getUserInfo()).andReturn("user:password").anyTimes();

    expect(httpURLConnection.getResponseCode()).andReturn(HttpURLConnection.HTTP_OK).anyTimes();

    // Make sure that the Authorization header is set with the correct value for "user:password"
    httpURLConnection.setRequestProperty("Authorization", "Basic dXNlcjpwYXNzd29yZA==");
    expectLastCall().once();

    expect(httpURLConnection.getInputStream()).andReturn(inputStream).anyTimes();

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

    List<String> allSubjects = restService.getAllSubjects();

    verify(inputStream);
    verify(httpURLConnection);

    assertEquals(1, allSubjects.size());
    assertEquals("abc", allSubjects.get(0));
  }
}
