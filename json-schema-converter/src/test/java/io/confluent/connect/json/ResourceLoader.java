/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.connect.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.IOException;
import java.io.InputStream;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ResourceLoader {

  public static final ResourceLoader DEFAULT = new ResourceLoader("/io/confluent/connect/json/");

  private final String rootPath;

  public ResourceLoader(String rootPath) {
    this.rootPath = requireNonNull(rootPath, "rootPath cannot be null");
  }

  public JSONObject readJSONObject(String relPath) throws IOException {
    try (InputStream stream = getStream(relPath)) {
      return new JSONObject(new JSONTokener(stream));
    }
  }

  public JsonNode readJsonNode(String relPath) throws IOException {
    try (InputStream stream = getStream(relPath)) {
      return new ObjectMapper().readTree(stream);
    }
  }

  public InputStream getStream(String relPath) {
    String absPath = rootPath + relPath;
    InputStream rval = getClass().getResourceAsStream(absPath);
    if (rval == null) {
      throw new IllegalArgumentException(format("failed to load resource by relPath [%s].\n"
          + "InputStream by path [%s] is null", relPath, absPath));
    }
    return rval;
  }

}
