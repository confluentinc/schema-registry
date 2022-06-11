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

package io.confluent.kafka.schemaregistry.maven.derive.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NumericNode;

import java.util.Comparator;

public class NumericNodeComparator implements Comparator<JsonNode> {
  @Override
  public int compare(JsonNode jsonNode1, JsonNode jsonNode2) {

    if (jsonNode1.equals(jsonNode2)) {
      return 0;
    }

    if ((jsonNode1 instanceof NumericNode) && (jsonNode2 instanceof NumericNode)) {
      Double doubleValue1 = jsonNode1.asDouble();
      Double doubleValue2 = jsonNode2.asDouble();
      if (doubleValue1.compareTo(doubleValue2) == 0) {
        return 0;
      }
    }
    
    return 1;
  }
}

