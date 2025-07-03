/*
 * Copyright 2014-2025 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.kafka.schemaregistry.json.jackson;

import com.fasterxml.jackson.core.util.VersionUtil;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.json.JSONArray;
import org.json.JSONObject;

public class JsonOrgModule extends SimpleModule {
  private static final long serialVersionUID = 1;

  private static final String NAME = "JsonOrgModule";

  public JsonOrgModule() {
    super(NAME, VersionUtil.versionFor(JsonOrgModule.class));
    addDeserializer(JSONArray.class, JSONArrayDeserializer.instance);
    addDeserializer(JSONObject.class, JSONObjectDeserializer.instance);
    addSerializer(JSONArraySerializer.instance);
    addSerializer(JSONObjectSerializer.instance);
  }
}