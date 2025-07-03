/*
 * Copyright 2021-2025 Confluent Inc.
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

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import io.confluent.kafka.schemaregistry.client.rest.UriBuilder.UriPercentEncoder;

public class UriBuilderTest {
  private static final Charset UTF8 = Charset.forName("UTF-8");

  @Test
  public void findTemplateNamesInPath() {
    List<String> templateNames = UriBuilder
        .findNamesInTemplate("https://some.host/{subject}/and/{version}/resource");
    assertEquals(templateNames, Arrays.asList("{subject}", "{version}"));
  }

  @Test
  public void encodeCommonPathSegmentChars() throws Exception {
    assertEquals("%0A", UriPercentEncoder.encode("\n", UTF8));
    assertEquals("%22", UriPercentEncoder.encode("\"", UTF8));
    assertEquals("%25", UriPercentEncoder.encode("%", UTF8));
    assertEquals("%3C", UriPercentEncoder.encode("<", UTF8));
    assertEquals("%3E", UriPercentEncoder.encode(">", UTF8));
    assertEquals("%5C", UriPercentEncoder.encode("\\", UTF8));
    assertEquals("%5E", UriPercentEncoder.encode("^", UTF8));
    assertEquals("%60", UriPercentEncoder.encode("`", UTF8));
    assertEquals("%7B", UriPercentEncoder.encode("{", UTF8));
    assertEquals("%7C", UriPercentEncoder.encode("|", UTF8));
    assertEquals("%7D", UriPercentEncoder.encode("}", UTF8));
    assertEquals("%3F", UriPercentEncoder.encode("?", UTF8));

    assertEquals("foo%20bar%3F", UriPercentEncoder.encode("foo bar?", UTF8));
  }

  @Test
  public void doNotEncodeAllowedPathSegmentChars() throws Exception {
    for (char c : UriPercentEncoder.CHARS_UNENCODE.toCharArray()) {
      String s = Character.toString(c);
      assertEquals(s, UriPercentEncoder.encode(s, UTF8));
    }
  }

  @Test
  public void verifyPathSegmentPercentEncodingOfCharactersHandledDifferentlyInLegacyRFCs()
      throws Exception {
    assertEquals("+", UriPercentEncoder.encode("+", UTF8)); // do not encode subdelim + (RFC 3986)
    assertEquals("%20", UriPercentEncoder.encode(" ", UTF8)); // do percent encode space (is supported in both path and query segments)
    assertEquals("~", UriPercentEncoder.encode("~", UTF8)); // do not encode subdelim ~ (RFC 3986)
    assertEquals("*", UriPercentEncoder.encode("*", UTF8)); // do not encode subdelim * (RFC 3986)
  }

  @Test
  public void buildPathTemplateParameters() throws Exception {
    UriBuilder uriBuilder = UriBuilder.fromPath("/some/site/{part}");
    URI uri = uriBuilder.build("mypart");
    assertEquals(uri, new URI("/some/site/mypart"));
  }

  @Test
  public void buildEncodedPathTemplateParameters() throws Exception {
    assertEquals(new URI("/some/site/my%20part"),
        UriBuilder.fromPath("/some/site/{part}").build("my part"));

    assertEquals(new URI("/some/site/my_part"),
        UriBuilder.fromPath("/some/site/{part}").build("my_part"));

    assertEquals(new URI("/some/site/my%3Cpart/more"),
        UriBuilder.fromPath("/some/site/{part}/more").build("my<part"));

    assertEquals(new URI("/some/site/my%2Fpart/more"),
        UriBuilder.fromPath("/some/site/{part}/more").build("my/part"));
  }

  @Test
  public void buildPathQueryParameters() throws Exception {
    UriBuilder uriBuilder = UriBuilder.fromPath("/some/site");
    uriBuilder.queryParam("first", "1");
    uriBuilder.queryParam("second", 2);
    assertEquals(uriBuilder.build(), new URI("/some/site?first=1&second=2"));
  }

  @Test
  public void testApi() throws Exception {
    String subject = "testTopic";
    int version = 2;
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}/versions/{version}")
        .queryParam("deleted", true);
    String path = builder.build(subject, version).toString();

    assertEquals(path, "/subjects/testTopic/versions/2?deleted=true");
  }

  @Test
  public void checkEncodingOfSomeStandardSubjectTopicNames() throws Exception {
    UriBuilder builder = UriBuilder.fromPath("/subjects/{subject}");
    assertEquals("/subjects/my1.topic.name", builder.build("my1.topic.name").toString());
    assertEquals("/subjects/another-topic-name", builder.build("another-topic-name").toString());
    assertEquals("/subjects/My_topic_name", builder.build("My_topic_name").toString());
  }

}
