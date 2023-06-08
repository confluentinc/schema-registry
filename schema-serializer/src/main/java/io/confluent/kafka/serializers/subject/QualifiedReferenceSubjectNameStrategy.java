/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.kafka.serializers.subject;

import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.serializers.subject.strategy.ReferenceSubjectNameStrategy;
import java.util.Map;

/**
 * An implementation of {@link ReferenceSubjectNameStrategy} that makes a reference name that
 * represents a path appear as a qualified name.
 */
public class QualifiedReferenceSubjectNameStrategy implements ReferenceSubjectNameStrategy {

  /**
   * Given a reference name, this method returns a subject name. The reference name here is simply
   * the path of the Protobuf file, with slashes replaced by dots, and the .proto suffix removed.
   *
   * @param refName name of the reference, the relative path of the Protobuf file
   * @param topic   the topic name
   * @param isKey   whether this reference is a key or a value
   * @param schema  the in-memory representation of the Protobuf schema
   * @return the subject name
   */
  @Override
  public String subjectName(String refName, String topic, boolean isKey, ParsedSchema schema) {
    return refName.replace(".proto", "").replace("/", ".");
  }

  @Override
  public void configure(Map<String, ?> map) {
  }
}