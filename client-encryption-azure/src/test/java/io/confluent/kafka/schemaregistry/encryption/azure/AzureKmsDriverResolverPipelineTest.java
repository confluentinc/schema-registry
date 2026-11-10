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

package io.confluent.kafka.schemaregistry.encryption.azure;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.azure.core.http.HttpClient;
import com.azure.core.http.HttpHeaderName;
import com.azure.core.http.HttpHeaders;
import com.azure.core.http.HttpPipeline;
import com.azure.core.http.HttpPipelineBuilder;
import com.azure.core.http.HttpRequest;
import com.azure.core.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * The versionless-key resolver must run on the pipeline it is given (the one carrying the
 * configured timeouts and retry policy), not on a KeyClient of its own with azure-core defaults.
 */
public class AzureKmsDriverResolverPipelineTest {

  private static final String VAULT = "https://vault.vault.azure.net";
  private static final String VERSION = "0123456789abcdef0123456789abcdef";

  @Test
  public void resolverUsesTheGivenPipeline() throws Exception {
    List<HttpRequest> seen = new CopyOnWriteArrayList<>();
    HttpClient fake = request -> {
      seen.add(request);
      String body = "{\"key\":{\"kid\":\"" + VAULT + "/keys/key1/" + VERSION + "\"}}";
      return Mono.just(new FakeResponse(request, 200, body));
    };
    // No credential policy: the fake accepts anything, which also proves no real credential is
    // needed when a pipeline is supplied.
    HttpPipeline pipeline = new HttpPipelineBuilder().httpClient(fake).build();

    String resolved = AzureKmsDriver.getVersionedKeyId(
        Collections.emptyMap(), VAULT + "/keys/key1", pipeline);

    assertEquals(VAULT + "/keys/key1/" + VERSION, resolved);
    assertEquals(1, seen.size());
    String url = seen.get(0).getUrl().toString();
    assertTrue(url, url.startsWith(VAULT + "/keys/key1"));
  }

  @Test
  public void versionedKeyNeedsNoLookup() throws Exception {
    HttpClient fake = request -> {
      throw new AssertionError("no request expected for an already versioned key");
    };
    HttpPipeline pipeline = new HttpPipelineBuilder().httpClient(fake).build();
    String versioned = VAULT + "/keys/key1/" + VERSION;
    assertEquals(versioned,
        AzureKmsDriver.getVersionedKeyId(Collections.emptyMap(), versioned, pipeline));
  }

  private static final class FakeResponse extends HttpResponse {
    private final int status;
    private final byte[] body;
    private final HttpHeaders headers = new HttpHeaders()
        .set(HttpHeaderName.CONTENT_TYPE, "application/json");

    FakeResponse(HttpRequest request, int status, String body) {
      super(request);
      this.status = status;
      this.body = body.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public int getStatusCode() {
      return status;
    }

    @Override
    public String getHeaderValue(String name) {
      return headers.getValue(HttpHeaderName.fromString(name));
    }

    @Override
    public HttpHeaders getHeaders() {
      return headers;
    }

    @Override
    public Flux<ByteBuffer> getBody() {
      return Flux.just(ByteBuffer.wrap(body));
    }

    @Override
    public Mono<byte[]> getBodyAsByteArray() {
      return Mono.just(body);
    }

    @Override
    public Mono<String> getBodyAsString() {
      return getBodyAsString(StandardCharsets.UTF_8);
    }

    @Override
    public Mono<String> getBodyAsString(Charset charset) {
      return Mono.just(new String(body, charset));
    }
  }
}
