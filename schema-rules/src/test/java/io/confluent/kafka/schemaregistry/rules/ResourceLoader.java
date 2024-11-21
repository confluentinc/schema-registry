/*
 * Copyright 2023 Confluent Inc.
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
 *
 */

package io.confluent.kafka.schemaregistry.rules;

import static java.util.Objects.requireNonNull;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class ResourceLoader {

  public static final ResourceLoader DEFAULT = new ResourceLoader("/");

  private final String rootPath;

  public ResourceLoader(String rootPath) {
    this.rootPath = requireNonNull(rootPath, "rootPath cannot be null");
  }

  public String toString(String relPath) {
    String absPath = rootPath + relPath;
    InputStream is = getClass().getResourceAsStream(absPath);
    if (is != null) {
      BufferedReader reader = new BufferedReader(new InputStreamReader(is));
      return reader.lines().collect(Collectors.joining(System.lineSeparator()));
    }
    return null;
  }
}
