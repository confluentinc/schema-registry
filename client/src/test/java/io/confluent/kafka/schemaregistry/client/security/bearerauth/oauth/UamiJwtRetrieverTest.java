/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.client.security.bearerauth.oauth;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.kafka.common.security.oauthbearer.internals.secured.UnretryableException;
import org.apache.kafka.common.utils.Utils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UamiJwtRetrieverTest {

  private static final String QUERY =
      "api-version=2025-04-07&resource=https%3A%2F%2Fconfluent.azure.com&client_id=uami-client-id";
  private static final long RETRY_BACKOFF_MS = 100L;
  private static final long RETRY_BACKOFF_MAX_MS = 1000L;

  // ---- get() tests ----

  @Test
  public void testGetReturnsResponseBody() throws IOException {
    String responseBody = buildImdsResponse("my-token");
    HttpURLConnection con = createConnection(200, responseBody);

    String result = UamiJwtRetriever.get(con, null, null);

    assertEquals(responseBody, result);
  }

  @Test
  public void testGetSetsGetMethod() throws IOException {
    HttpURLConnection con = createConnection(200, buildImdsResponse("token"));

    UamiJwtRetriever.get(con, null, null);

    verify(con).setRequestMethod("GET");
  }

  @Test
  public void testGetSetsRequiredHeaders() throws IOException {
    HttpURLConnection con = createConnection(200, buildImdsResponse("token"));

    UamiJwtRetriever.get(con, null, null);

    verify(con).setRequestProperty(UamiJwtRetriever.METADATA_HEADER, "true");
    verify(con).setRequestProperty("Accept", "application/json");
    verify(con).setRequestProperty("Cache-Control", "no-cache");
  }

  @Test
  public void testGetAppliesTimeoutsWhenSet() throws IOException {
    HttpURLConnection con = createConnection(200, buildImdsResponse("token"));

    UamiJwtRetriever.get(con, 5000, 3000);

    verify(con).setConnectTimeout(5000);
    verify(con).setReadTimeout(3000);
  }

  @Test
  public void testGetCallsConnectAndDisablesCache() throws IOException {
    HttpURLConnection con = createConnection(200, buildImdsResponse("token"));

    UamiJwtRetriever.get(con, null, null);

    verify(con).connect();
    verify(con).setUseCaches(false);
  }

  @Test
  public void testGetSkipsTimeoutsWhenNull() throws IOException {
    HttpURLConnection con = createConnection(200, buildImdsResponse("token"));

    UamiJwtRetriever.get(con, null, null);

    verify(con, never()).setConnectTimeout(5000);
    verify(con, never()).setReadTimeout(3000);
  }

  @Test
  public void testGetThrowsUnretryableExceptionOnBadRequest() throws IOException {
    HttpURLConnection con = createErrorConnection(HttpURLConnection.HTTP_BAD_REQUEST,
        "{\"error\":\"invalid_request\", \"error_description\":\"bad parameter\"}");

    assertThrows(UnretryableException.class, () -> UamiJwtRetriever.get(con, null, null));
  }

  @Test
  public void testGetThrowsUnretryableExceptionOnUnauthorized() throws IOException {
    HttpURLConnection con = createErrorConnection(HttpURLConnection.HTTP_UNAUTHORIZED,
        "{\"error\":\"unauthorized\", \"error_description\":\"no access\"}");

    assertThrows(UnretryableException.class, () -> UamiJwtRetriever.get(con, null, null));
  }

  @Test
  public void testGetThrowsUnretryableExceptionOnForbidden() throws IOException {
    HttpURLConnection con = createErrorConnection(HttpURLConnection.HTTP_FORBIDDEN,
        "{\"error\":\"forbidden\", \"error_description\":\"no permission\"}");

    assertThrows(UnretryableException.class, () -> UamiJwtRetriever.get(con, null, null));
  }

  @Test
  public void testGetThrowsRetryableIOExceptionOnServerError() throws IOException {
    HttpURLConnection con = createErrorConnection(HttpURLConnection.HTTP_INTERNAL_ERROR,
        "{\"error\":\"server_error\", \"error_description\":\"try again\"}");

    // Asserting IOException (not UnretryableException) is sufficient to prove retryability
    assertThrows(IOException.class, () -> UamiJwtRetriever.get(con, null, null));
  }

  @Test
  public void testGetThrowsIOExceptionWhenInputStreamUnreadable() throws IOException {
    HttpURLConnection con = mock(HttpURLConnection.class);
    when(con.getURL()).thenReturn(new URL(UamiJwtRetriever.DEFAULT_IMDS_ENDPOINT));
    when(con.getResponseCode()).thenReturn(200);
    when(con.getInputStream()).thenThrow(new IOException("stream unavailable"));

    assertThrows(IOException.class, () -> UamiJwtRetriever.get(con, null, null));
  }

  @Test
  public void testGetThrowsIOExceptionOnEmptySuccessResponse() throws IOException {
    HttpURLConnection con = createConnection(200, "");

    assertThrows(IOException.class, () -> UamiJwtRetriever.get(con, null, null));
  }

  // ---- buildRequestUrl() tests ----

  @Test
  public void testBuildRequestUrlAppendsQueryToEndpoint() {
    UamiJwtRetriever retriever = buildRetriever(QUERY,
        UamiJwtRetriever.DEFAULT_IMDS_ENDPOINT);

    String url = retriever.buildRequestUrl();

    assertEquals(UamiJwtRetriever.DEFAULT_IMDS_ENDPOINT + "?" + QUERY, url);
  }

  @Test
  public void testBuildRequestUrlUsesCustomEndpoint() {
    String customEndpoint = "http://localhost:40342/metadata/identity/oauth2/token";
    UamiJwtRetriever retriever = buildRetriever(QUERY, customEndpoint);

    String url = retriever.buildRequestUrl();

    assertTrue(url.startsWith(customEndpoint + "?"));
    assertTrue(url.endsWith(QUERY));
  }

  @Test
  public void testBuildRequestUrlPreservesQueryAsIs() {
    String rawQuery = "api-version=2025-04-07&resource=https%3A%2F%2Fexample.com&custom_param=val";
    UamiJwtRetriever retriever = buildRetriever(rawQuery,
        UamiJwtRetriever.DEFAULT_IMDS_ENDPOINT);

    String url = retriever.buildRequestUrl();

    assertEquals(UamiJwtRetriever.DEFAULT_IMDS_ENDPOINT + "?" + rawQuery, url);
  }

  // ---- constructor null checks ----

  @Test
  public void testConstructorRejectsNullQuery() {
    assertThrows(NullPointerException.class,
        () -> new UamiJwtRetriever(null,
            UamiJwtRetriever.DEFAULT_IMDS_ENDPOINT, RETRY_BACKOFF_MS, RETRY_BACKOFF_MAX_MS,
            null, null));
  }

  @Test
  public void testConstructorRejectsNullEndpointUrl() {
    assertThrows(NullPointerException.class,
        () -> new UamiJwtRetriever(QUERY,
            null, RETRY_BACKOFF_MS, RETRY_BACKOFF_MAX_MS, null, null));
  }

  // ---- helpers ----

  private UamiJwtRetriever buildRetriever(String query, String endpointUrl) {
    return new UamiJwtRetriever(query, endpointUrl,
        RETRY_BACKOFF_MS, RETRY_BACKOFF_MAX_MS, null, null);
  }

  private String buildImdsResponse(String accessToken) {
    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node = mapper.createObjectNode();
    node.put("access_token", accessToken);
    node.put("expires_in", "3599");
    node.put("token_type", "Bearer");
    return node.toString();
  }

  private HttpURLConnection createConnection(int responseCode, String responseBody)
      throws IOException {
    HttpURLConnection con = mock(HttpURLConnection.class);
    when(con.getURL()).thenReturn(new URL(UamiJwtRetriever.DEFAULT_IMDS_ENDPOINT));
    when(con.getResponseCode()).thenReturn(responseCode);
    when(con.getInputStream())
        .thenReturn(new ByteArrayInputStream(Utils.utf8(responseBody)));
    return con;
  }

  private HttpURLConnection createErrorConnection(int responseCode, String errorBody)
      throws IOException {
    HttpURLConnection con = mock(HttpURLConnection.class);
    when(con.getURL()).thenReturn(new URL(UamiJwtRetriever.DEFAULT_IMDS_ENDPOINT));
    when(con.getResponseCode()).thenReturn(responseCode);
    when(con.getInputStream()).thenThrow(new IOException("HTTP " + responseCode));
    when(con.getErrorStream())
        .thenReturn(new ByteArrayInputStream(errorBody.getBytes(StandardCharsets.UTF_8)));
    return con;
  }
}
