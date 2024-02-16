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
package io.confluent.kafka.schemaregistry.rules.cel.avro;

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

import example.avro.Kind;
import example.avro.User;
import java.util.Collections;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptHost;

public class AvroScriptHostTest {

  @Test
  public void testSimple() throws Exception {
    ScriptHost scriptHost = ScriptHost.newBuilder().registry(AvroRegistry.newRegistry()).build();

    Script script =
        scriptHost
            .buildScript("user.name == 'foobar' && user.kind == \"TWO\"")
            .withDeclarations(Decls.newVar("user", Decls.newObjectType(User.SCHEMA$.getFullName())))
            .withTypes(User.SCHEMA$)
            .build();

    User userMatch = new User("foobar", Collections.emptyList(), Kind.TWO);
    User userNoMatch = new User("foobaz", Collections.emptyList(), Kind.THREE);

    assertTrue(script.execute(Boolean.class, singletonMap("user", userMatch)));
    assertFalse(script.execute(Boolean.class, singletonMap("user", userNoMatch)));

    GenericRecord userMatch2 = new GenericData.Record(User.SCHEMA$);
    userMatch2.put("name", "foobar");
    userMatch2.put("kind", new GenericData.EnumSymbol(Kind.SCHEMA$, "TWO"));

    GenericRecord userNoMatch2 = new GenericData.Record(User.SCHEMA$);
    userNoMatch2.put("name", "foobaz");
    userNoMatch2.put("kind", new GenericData.EnumSymbol(Kind.SCHEMA$, "THREE"));

    assertTrue(script.execute(Boolean.class, singletonMap("user", userMatch2)));
    assertFalse(script.execute(Boolean.class, singletonMap("user", userNoMatch2)));
  }

  @Test
  public void testComplexInput() throws Exception {
    ScriptHost scriptHost = ScriptHost.newBuilder().registry(AvroRegistry.newRegistry()).build();

    Script script =
        scriptHost
            .buildScript("user.friends[0].kind == \"TWO\"")
            .withDeclarations(Decls.newVar("user", Decls.newObjectType(User.SCHEMA$.getFullName())))
            .withTypes(User.SCHEMA$)
            .build();

    User friend = new User("friend", Collections.emptyList(), Kind.TWO);
    User user = new User("foobar", Collections.singletonList(friend), Kind.ONE);

    assertTrue(script.execute(Boolean.class, singletonMap("user", user)));

    GenericRecord friend2 = new GenericData.Record(User.SCHEMA$);
    friend2.put("name", "friend");
    friend2.put("kind", new GenericData.EnumSymbol(Kind.SCHEMA$, "TWO"));

    GenericRecord user2 = new GenericData.Record(User.SCHEMA$);
    user2.put("name", "foobar");
    user2.put("kind", new GenericData.EnumSymbol(Kind.SCHEMA$, "ONE"));
    user2.put("friends", Collections.singletonList(friend));

    assertTrue(script.execute(Boolean.class, singletonMap("user", user2)));

    // return the enum

    script =
        scriptHost
            .buildScript("user.friends[0].kind")
            .withDeclarations(Decls.newVar("user", Decls.newObjectType(User.SCHEMA$.getFullName())))
            .withTypes(User.SCHEMA$)
            .build();

    assertEquals(script.execute(String.class, singletonMap("user", user)), "TWO");

    assertEquals(script.execute(String.class, singletonMap("user", user2)), "TWO");
  }
}
