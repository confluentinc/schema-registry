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

public class ConvertRequest {
  /**
   * Single-document mode: the DDL text. Ignored when {@link #documents} is non-empty.
   */
  public String sql;
  public String target;
  public String rowName;

  /**
   * Multi-document mode: each document with a name and DDL.
   */
  public List<Document> documents;
  /**
   * Multi-document mode: the {@link Document#name name} of the document to convert.
   */
  public String activeDocument;

  public static class Document {
    public String name;
    public String ddl;
  }
}
