/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.kafka.schemaregistry.rest.filters;

import java.net.URI;
import java.util.Collections;
import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.UriBuilder;
import org.junit.Assert;
import org.junit.Test;

public class ContextFilterTest {

  ContextFilter contextFilter = new ContextFilter();

  @Test
  public void testContextsRoot() {
    String path = "/contexts/";
    Assert.assertEquals(
        "URI most not change",
        "/contexts/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testSubjectPartOfUri() {
    String path = "/contexts/.test-ctx/subjects/test-subject/versions";
    Assert.assertEquals(
        "Subject must be prefixed",
        "/subjects/:.test-ctx:test-subject/versions/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testSubjectPartOfUriDefaultContext() {
    String path = "/contexts/:.:/subjects/test-subject/versions";
    Assert.assertEquals(
        "Subject must be prefixed",
        "/subjects/test-subject/versions/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testNoSubjectDefaultContext() {
    String path = "/contexts/:.:/subjects";
    Assert.assertEquals(
        "Subject must be prefixed",
        "/subjects/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testMissingLeadingDotInContext() {
    String path = "/contexts/test-ctx/subjects/test-subject/versions";
    Assert.assertEquals(
        "Subject must be prefixed",
        "/subjects/:.test-ctx:test-subject/versions/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testUriEndsWithSubject() {
    String path = "/contexts/.test-ctx/subjects/test-subject/";
    Assert.assertEquals(
        "Subject must be prefixed",
        "/subjects/:.test-ctx:test-subject/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testUriWithIds() {
    String path = "/contexts/.test-ctx/schemas/ids/1/";
    URI uri = contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>());
    Assert.assertEquals(
        "URI must not change",
        "/schemas/ids/1/",
        uri.getPath()
    );
    Assert.assertEquals(
        "Query param must change",
        "subject=:.test-ctx:",
        uri.getQuery()
    );
  }

  @Test
  public void testWildcardContextUnmodified() {
    String path = "/contexts/:.:/schemas/";
    UriBuilder uriBuilder = UriBuilder.fromPath(path);
    uriBuilder.queryParam("subjectPrefix", ":*:");
    MultivaluedMap<String, String> queryParams = new MultivaluedHashMap<>();
    queryParams.put("subjectPrefix", Collections.singletonList(":*:"));
    URI uri = contextFilter.modifyUri(uriBuilder, path, queryParams);
    Assert.assertEquals(
        "URI must not change",
        "/schemas/",
        uri.getPath()
    );
    Assert.assertEquals(
        "Query param must not change",
        "subjectPrefix=:*:",
        uri.getQuery()
    );
  }

  @Test
  public void testUriWithEncodedSlash() {
    String path = "/contexts/.test-ctx/subjects/slash%2Fin%2Fmiddle/";
    Assert.assertEquals(
        "Subject must be prefixed",
        "/subjects/:.test-ctx:slash%2Fin%2Fmiddle/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getRawPath()
    );
  }

  @Test
  public void testConfigUriWithSubject() {
    String path = "/contexts/.test-ctx/config/test-subject";
    Assert.assertEquals(
        "Subject must be prefixed",
        "/config/:.test-ctx:test-subject/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testConfigUriWithoutSubject() {
    String path = "/contexts/.test-ctx/config";
    Assert.assertEquals(
        "Wildcard must be prefixed",
        "/config/:.test-ctx:/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testUriWithoutModification() {
    String path = "/chc/live";
    Assert.assertEquals(
        "URI must not change",
        "/chc/live/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testModeUriWithSubject() {
    String path = "/contexts/.test-ctx/mode/test-subject";
    Assert.assertEquals(
        "Subject must be prefixed",
        "/mode/:.test-ctx:test-subject/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testModeUriWithoutSubject() {
    String path = "/contexts/.test-ctx/mode";
    Assert.assertEquals(
        "Mode must be prefixed",
        "/mode/:.test-ctx:/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testModeUriWithoutSubjectDefaultContext() {
    String path = "/contexts/:.:/mode";
    Assert.assertEquals(
        "Mode must not be prefixed",
        "/mode/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testConfigNotRoot() {
    String path = "/other/config/test";
    Assert.assertEquals(
        "Non-root config must be unmodified",
        "/other/config/test/",
        contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

  @Test
  public void testInvalidContext() {
    String path = "/contexts/foo:bar/subjects";
    Assert.assertThrows(
        "Invalid context must be rejected",
        IllegalArgumentException.class,
        () -> contextFilter.modifyUri(UriBuilder.fromPath(path), path, new MultivaluedHashMap<>()).getPath()
    );
  }

}
