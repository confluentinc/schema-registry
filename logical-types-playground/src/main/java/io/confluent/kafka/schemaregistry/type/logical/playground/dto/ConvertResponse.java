/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.kafka.schemaregistry.type.logical.playground.dto;

import java.util.List;

public class ConvertResponse {
  public String schema;
  public List<Marker> errors;

  public static class Marker {
    public int line;
    public int column;
    public int endLine;
    public int endColumn;
    public String message;

    public Marker() {
    }

    public Marker(int line, int column, int endLine, int endColumn, String message) {
      this.line = line;
      this.column = column;
      this.endLine = endLine;
      this.endColumn = endColumn;
      this.message = message;
    }
  }
}
