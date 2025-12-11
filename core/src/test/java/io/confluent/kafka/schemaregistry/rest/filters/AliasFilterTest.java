/*
 * Copyright 2023 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.rest.filters;

import static io.confluent.kafka.schemaregistry.utils.QualifiedSubject.DEFAULT_TENANT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.rest.entities.Config;

import java.net.URI;
import java.util.Collections;

import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.UriBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AliasFilterTest {

  private AliasFilter aliasFilter;


  @Before
  public void setUp() throws Exception {
    SchemaRegistry schemaRegistry = mock(SchemaRegistry.class);
    when(schemaRegistry.tenant()).thenReturn(DEFAULT_TENANT);
    aliasFilter = new AliasFilter(schemaRegistry);

    Config config = new Config();
    config.setAlias("mySubject");
    when(schemaRegistry.getConfig("myAlias")).thenReturn(config);
    Config config2 = new Config();
    config2.setAlias("mySubject2");
    when(schemaRegistry.getConfig("slash/in/middle")).thenReturn(config2);
  }

  @Test
  public void testRoot() {
    String path = "/";
    Assert.assertEquals(
        "URI must not change",
        "/",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testSubjectPartOfUri() {
    String path = "/subjects/myAlias/versions";
    Assert.assertEquals(
        "Subject must be replaced",
        "/subjects/mySubject/versions",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testNoSubjectDefaultContext() {
    String path = "/subjects";
    Assert.assertEquals(
        "Subject must not change",
        "/subjects",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testUriEndsWithSubject() {
    String path = "/subjects/myAlias/";
    Assert.assertEquals(
        "Subject must be replaced",
        "/subjects/mySubject/",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testUriWithIds() {
    String path = "/schemas/ids/1";
    URI uri = aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>());
    Assert.assertEquals(
        "URI must not change",
        "/schemas/ids/1",
        uri.getPath()
    );
    Assert.assertEquals(
        "Query param must not change",
        "subject=",
        uri.getQuery()
    );
  }

  @Test
  public void testUriWithIdsAndSubject() {
    String path = "/schemas/ids/1/";
    UriBuilder uriBuilder = UriBuilder.fromPath(path);
    uriBuilder.queryParam("subject", "myAlias");
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.put("subject", Collections.singletonList("myAlias"));
    URI uri = aliasFilter.modifyUri(uriBuilder, path, queryParams);
    Assert.assertEquals(
        "URI must not change",
        "/schemas/ids/1/",
        uri.getPath()
    );
    Assert.assertEquals(
        "Query param must match",
        "subject=mySubject",
        uri.getQuery()
    );
  }

  @Test
  public void testUriWithEncodedSlash() {
    String path = "/subjects/slash%2Fin%2Fmiddle/";
    Assert.assertEquals(
        "Subject must be replaced",
        "/subjects/mySubject2/",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testConfigUriWithSubject() {
    String path = "/config/myAlias";
    Assert.assertEquals(
        "Subject must not change",
        "/config/myAlias",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testUriWithoutModification() {
    String path = "/chc/live";
    Assert.assertEquals(
        "URI must not change",
        "/chc/live",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testModeUriWithSubject() {
    String path = "/mode/myAlias";
    Assert.assertEquals(
        "Subject must not change",
        "/mode/myAlias",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testDekPartOfUri() {
    String path = "/dek-registry/v1/keks/test-kek/deks/myAlias";
    Assert.assertEquals(
        "Subject must be replaced",
        "/dek-registry/v1/keks/test-kek/deks/mySubject",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

}
