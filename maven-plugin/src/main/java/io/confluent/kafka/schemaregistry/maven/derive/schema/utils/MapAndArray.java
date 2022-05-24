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

package io.confluent.kafka.schemaregistry.maven.derive.schema.utils;

import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Helper class to return information about Schemas for Multiple messages.
 * Returns List of schemas and a map containing information on how many messages each schema maps
 * Kept separate to enhance further or add more details.
 */
public class MapAndArray {
  ArrayList<JSONObject> schemas;
  List<List<Integer>> schemaToMessagesInfo;

  public MapAndArray(ArrayList<JSONObject> schemas, List<List<Integer>> schemaToMessagesInfo) {
    this.schemas = schemas;
    this.schemaToMessagesInfo = schemaToMessagesInfo;
  }

  public ArrayList<JSONObject> getSchemas() {
    return schemas;
  }

  public List<List<Integer>> getSchemaToMessagesInfo() {
    return schemaToMessagesInfo;
  }
}
