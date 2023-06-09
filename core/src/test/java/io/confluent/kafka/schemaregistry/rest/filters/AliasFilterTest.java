/*
 * Copyright 2023 Confluent Inc.
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

public class AliasFilterTest {

  private AliasFilter aliasFilter;


  @Before
  public void setUp() throws Exception {
    KafkaSchemaRegistry schemaRegistry = mock(KafkaSchemaRegistry.class);
    aliasFilter = new AliasFilter(schemaRegistry);

    Config config = new Config();
    config.setAlias("mySubject");
    when(schemaRegistry.getConfig("myAlias")).thenReturn(config);
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
}
