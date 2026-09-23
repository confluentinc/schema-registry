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

package io.confluent.kafka.schemaregistry.type.logical.provenance;

import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceField;
import io.confluent.kafka.schemaregistry.client.rest.entities.ProvenanceVersion;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaProvenance;

import java.util.ArrayList;
import java.util.List;

/**
 * Turns a {@link ProvenanceReport} into the {@link SchemaProvenance} the REST endpoint serves.
 *
 * <p>The one place the in-process result becomes the wire contract, shared by the endpoint and by
 * anything standing in for it in-process, so a test double speaks exactly the production shape
 * rather than a shortcut around it.
 */
public final class SchemaProvenanceEncoder {

  private SchemaProvenanceEncoder() {
  }

  /**
   * The wire form of {@code report}.
   *
   * @param schemaIds the schema id of each version, in the report's order
   * @param versions the version number of each version, or {@code null} where the caller does not
   *     know them, as a caller computing from schemas alone does not
   * @throws IllegalArgumentException if a list does not have one entry per version
   */
  public static SchemaProvenance encode(String subject, ProvenanceReport report,
      List<Integer> schemaIds, List<Integer> versions) {
    List<ProvenanceReport.Version> reported = report.getVersions();
    if (schemaIds.size() != reported.size()
        || (versions != null && versions.size() != reported.size())) {
      throw new IllegalArgumentException("Expected one schema id and version per version, got "
          + reported.size() + " versions, " + schemaIds.size() + " schema ids and "
          + (versions == null ? "no" : versions.size()) + " version numbers");
    }
    List<ProvenanceVersion> encoded = new ArrayList<>(reported.size());
    for (int i = 0; i < reported.size(); i++) {
      List<ProvenanceField> fields = new ArrayList<>();
      for (ProvenanceReport.Member member : reported.get(i).getMembers()) {
        fields.add(new ProvenanceField(
            member.getPath(),
            member.getNames(),
            member.getId()));
      }
      encoded.add(new ProvenanceVersion(
          versions == null ? null : versions.get(i), schemaIds.get(i), fields));
    }
    return new SchemaProvenance(subject, encoded);
  }
}
