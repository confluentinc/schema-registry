/*
 * Copyright 2025 Confluent Inc.
 */

package io.confluent.kafka.schemaregistry.rest.filters;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.confluent.kafka.schemaregistry.client.rest.entities.Config;
import io.confluent.kafka.schemaregistry.storage.KafkaSchemaRegistry;
import java.net.URI;
import java.util.Collections;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AliasNonDefaultTenantFilterTest {

  private AliasFilter aliasFilter;


  @Before
  public void setUp() throws Exception {
    KafkaSchemaRegistry schemaRegistry = mock(KafkaSchemaRegistry.class);
    when(schemaRegistry.tenant()).thenReturn("myTenant");
    aliasFilter = new AliasFilter(schemaRegistry);

    Config config = new Config();
    config.setAlias("mySubject");
    when(schemaRegistry.getConfig("myTenant_myAlias")).thenReturn(config);
    Config config2 = new Config();
    config2.setAlias("mySubject2");
    when(schemaRegistry.getConfig("myTenant_slash/in/middle")).thenReturn(config2);
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
    String path = "/subjects/myTenant_myAlias/versions";
    Assert.assertEquals(
        "Subject must be replaced",
        "/subjects/myTenant_mySubject/versions",
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
    String path = "/subjects/myTenant_myAlias/";
    Assert.assertEquals(
        "Subject must be replaced",
        "/subjects/myTenant_mySubject/",
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
    String path = "/subjects/myTenant_slash%2Fin%2Fmiddle/";
    Assert.assertEquals(
        "Subject must be replaced",
        "/subjects/myTenant_mySubject2/",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testConfigUriWithSubject() {
    String path = "/config/myTenant_myAlias";
    Assert.assertEquals(
        "Subject must not change",
        "/config/myTenant_myAlias",
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
    String path = "/mode/myTenant_myAlias";
    Assert.assertEquals(
        "Subject must not change",
        "/mode/myTenant_myAlias",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testDekPartOfUri() {
    // Note that dek subjects are not qualified
    String path = "/dek-registry/v1/keks/test-kek/deks/myAlias";
    Assert.assertEquals(
        "Subject must be replaced",
        "/dek-registry/v1/keks/test-kek/deks/mySubject",
        aliasFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

}
