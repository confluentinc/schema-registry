/*
 * Copyright 2022 Confluent Inc.
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
package io.confluent.kafka.schemaregistry.client.rest.entities;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.common.collect.ImmutableList;
import io.confluent.kafka.schemaregistry.rules.RuleException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

public class MergeEntitiesTest {

  @Test
  public void mergeMetadatas() throws Exception {
    Map<String, Set<String>> tags = new HashMap<>();
    tags.put("**.ssn", Collections.singleton("PII"));
    Map<String, String> properties = new HashMap<>();
    properties.put("key1", "value1");
    Set<String> sensitive = new HashSet<>();
    sensitive.add("key1");
    Metadata m1 = new Metadata(tags, properties, sensitive);
    Metadata m2 = new Metadata(Collections.emptyMap(), Collections.emptyMap(), null);

    Metadata m3 = Metadata.mergeMetadata(m1, m2);
    assertEquals(m3.getTags().get("**.ssn"), Collections.singleton("PII"));
    assertEquals(m3.getProperties().get("key1"), "value1");
    assertTrue(m3.getSensitive().contains("key1"));

    tags = new HashMap<>();
    tags.put("**.ssn", Collections.singleton("PRIVATE"));
    properties = new HashMap<>();
    properties.put("key2", "value2");
    sensitive = new HashSet<>();
    sensitive.add("key2");
    m2 = new Metadata(tags, properties, sensitive);

    Metadata m4 = Metadata.mergeMetadata(m1, m2);
    assertEquals(m4.getTags().get("**.ssn"), Collections.singleton("PRIVATE"));
    assertEquals(m4.getProperties().get("key1"), "value1");
    assertEquals(m4.getProperties().get("key2"), "value2");
    assertTrue(m4.getSensitive().contains("key1"));
    assertTrue(m4.getSensitive().contains("key2"));
  }

  @Test
  public void mergeRuleSets() throws Exception {
    Rule r1 = new Rule("hi", null, null, null, null, null, null, null, null, null, false);
    Rule r2 = new Rule("bye", null, null, null, null, null, null, null, null, null, false);
    List<Rule> rules1 = ImmutableList.of(r1, r2);
    RuleSet rs1 = new RuleSet(rules1, null);
    List<Rule> rules2 = ImmutableList.of(r2, r1);
    RuleSet rs2 = new RuleSet(rules2, null);

    RuleSet rs3 = RuleSet.mergeRuleSets(rs1, rs2);
    assertEquals(rs3.getMigrationRules(), rules2);

    rs3 = RuleSet.mergeRuleSets(rs2, rs1);
    assertEquals(rs3.getMigrationRules(), rules1);
  }

  @Test
  public void invalidRuleSets() throws Exception {
    Rule r1 = new Rule(null, null, null, null, "DUMMY", null, null, null, null, null, false);
    try {
      r1.validate();
      fail();
    } catch (RuleException e) {
      assertEquals("Missing rule name", e.getMessage());
    }

    Rule r2 = new Rule("", null, null, null, "DUMMY", null, null, null, null, null, false);
    try {
      r2.validate();
      fail();
    } catch (RuleException e) {
      assertEquals("Empty rule name", e.getMessage());
    }

    Rule r3 = new Rule("0", null, null, null, "DUMMY", null, null, null, null, null, false);
    try {
      r3.validate();
      fail();
    } catch (RuleException e) {
      assertTrue("Illegal ", e.getMessage().startsWith("Illegal initial character"));
    }

    Rule r4 = new Rule("With space", null, null, null, "DUMMY", null, null, null, null, null, false);
    try {
      r4.validate();
      fail();
    } catch (RuleException e) {
      assertTrue("Illegal ", e.getMessage().startsWith("Illegal character"));
    }
  }
}
