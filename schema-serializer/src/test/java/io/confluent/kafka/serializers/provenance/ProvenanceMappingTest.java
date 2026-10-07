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

package io.confluent.kafka.serializers.provenance;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.common.errors.SerializationException;
import org.junit.Test;

/** A response that does not answer the request asked is the registry's inconsistency. */
public class ProvenanceMappingTest {

  @Test
  public void aResponseWithoutARequestedVersionIsRejected() {
    SerializationException e = assertThrows(SerializationException.class,
        () -> ProvenanceMapping.join(response(field(1, "a"), field(1, "a")), 1, 3));
    assertTrue(e.getMessage(), e.getMessage().contains("schema id 3"));
  }

  @Test
  public void aLocationWithoutNamesIsRejected() {
    ProvenanceMapping mapping = ProvenanceMapping.join(
        response(field(1, "a"), new ProvenanceField(Arrays.asList(7), null, 1)), 1, 2);
    SerializationException e = assertThrows(SerializationException.class, mapping::requireNames);
    assertTrue(e.getMessage(), e.getMessage().contains("location [7] of schema id 2"));
  }

  @Test
  public void aLocationWithoutAPidIsRejected() {
    SerializationException e = assertThrows(SerializationException.class,
        () -> ProvenanceMapping.join(response(field(1, "a"),
            new ProvenanceField(Arrays.asList(0), Arrays.asList("a"), null)), 1, 2));
    assertTrue(e.getMessage(), e.getMessage().contains("has no provenance id"));
  }

  @Test
  public void aPidSharedByTwoLocationsIsRejected() {
    // Pairing is by pid: two writer locations sharing one would pair the reader with either.
    SchemaProvenance response = new SchemaProvenance("s", Arrays.asList(
        new ProvenanceVersion(1, 1, Arrays.asList(field(1, "a"),
            new ProvenanceField(Arrays.asList(2), Arrays.asList("b"), 1))),
        new ProvenanceVersion(2, 2, Collections.singletonList(field(1, "a")))));
    SerializationException e = assertThrows(SerializationException.class,
        () -> ProvenanceMapping.join(response, 1, 2));
    assertTrue(e.getMessage(), e.getMessage().contains("shares provenance id 1"));
  }

  @Test
  public void aBranchIsToldByTheKindItSitsIn() {
    // Below a collection step the type a location sits in is no location: the enclosing
    // location's kind tells it, as the root's does at the top.
    ProvenanceVersion reader = new ProvenanceVersion(2, 2, "STRUCT", Arrays.asList(
        kinded(path(0), "ARRAY<UNION>", 1),
        kinded(path(0, 0, 0), "SCALAR", 2),
        kinded(path(0, 0, 1), "STRUCT", 3),
        kinded(path(0, 0, 1, 0), "SCALAR", 4),
        kinded(path(1), "MAP<SCALAR, ARRAY<STRUCT>>", 5),
        kinded(path(1, 1, 0, 0), "SCALAR", 6)));
    ProvenanceVersion writer = new ProvenanceVersion(1, 1, "UNION", Arrays.asList(
        kinded(path(0), "STRUCT", 7),
        kinded(path(0, 0), "SCALAR", 8)));
    ProvenanceMapping mapping = ProvenanceMapping.join(
        new SchemaProvenance("s", Arrays.asList(writer, reader)), 1, 2);
    mapping.requireKinds();

    assertFalse(mapping.isReaderBranch(path(0)));
    assertTrue(mapping.isReaderBranch(path(0, 0, 0)));
    assertTrue(mapping.isReaderBranch(path(0, 0, 1)));
    assertFalse(mapping.isReaderBranch(path(0, 0, 1, 0)));
    assertFalse(mapping.isReaderBranch(path(1, 1, 0, 0)));
    assertTrue(mapping.isWriterBranch(path(0)));
    assertFalse(mapping.isWriterBranch(path(0, 0)));
    assertEquals("MAP<SCALAR, ARRAY<STRUCT>>", mapping.readerKindOf(path(1)));
  }

  @Test
  public void aLocationOrARootWithoutAKindIsRejected() {
    ProvenanceMapping location = ProvenanceMapping.join(new SchemaProvenance("s", Arrays.asList(
        new ProvenanceVersion(1, 1, "STRUCT", Collections.singletonList(field(1, "a"))),
        new ProvenanceVersion(2, 2, "STRUCT", Collections.singletonList(
            kinded(path(0), "SCALAR", 1))))), 1, 2);
    SerializationException e = assertThrows(SerializationException.class, location::requireKinds);
    assertTrue(e.getMessage(), e.getMessage().contains("location [1] of schema id 1"));

    ProvenanceMapping root = ProvenanceMapping.join(response(
        kinded(path(0), "SCALAR", 1), kinded(path(0), "SCALAR", 1)), 1, 2);
    e = assertThrows(SerializationException.class, root::requireKinds);
    assertTrue(e.getMessage(), e.getMessage().contains("root of schema id 1"));
  }

  @Test
  public void aMalformedKindIsRejected() {
    ProvenanceMapping mapping = ProvenanceMapping.join(new SchemaProvenance("s", Arrays.asList(
        new ProvenanceVersion(1, 1, "STRUCT", Collections.singletonList(
            kinded(path(0), "ARRAY<UNION", 1))),
        new ProvenanceVersion(2, 2, "STRUCT", Collections.singletonList(
            kinded(path(0), "SCALAR", 1))))), 1, 2);
    SerializationException e = assertThrows(SerializationException.class,
        () -> mapping.isWriterBranch(path(0, 0, 0)));
    assertTrue(e.getMessage(), e.getMessage().contains("malformed kind: ARRAY<UNION"));
  }

  private static ProvenanceField kinded(List<Integer> path, String kind, int pid) {
    return new ProvenanceField(path, Arrays.asList("n" + pid), kind, pid);
  }

  private static List<Integer> path(Integer... steps) {
    return Arrays.asList(steps);
  }

  private static SchemaProvenance response(ProvenanceField writer, ProvenanceField reader) {
    return new SchemaProvenance("s", Arrays.asList(
        new ProvenanceVersion(1, 1, Collections.singletonList(writer)),
        new ProvenanceVersion(2, 2, Collections.singletonList(reader))));
  }

  private static ProvenanceField field(int pid, String name) {
    return new ProvenanceField(Arrays.asList(pid), Arrays.asList(name), pid);
  }
}
