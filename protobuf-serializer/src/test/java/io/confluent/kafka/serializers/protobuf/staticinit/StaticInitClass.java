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

package io.confluent.kafka.serializers.protobuf.staticinit;

/**
 * A class that is not a protobuf message, but declares a {@code getDescriptor()} method with the
 * wrong return type, so it can stand in for a protobuf class under a reflection-based type check.
 * Its static initializer records that it ran, by setting a system property, so tests can assert
 * that a schema-derived class name is never initialized before being checked. Tests must not
 * reference this class directly, since merely referring to it would run the static initializer.
 */
public class StaticInitClass {

  public static final String PROPERTY = "io.confluent.test.protobuf.static.initializer.ran";

  static {
    System.setProperty(PROPERTY, "ran");
  }

  public static String getDescriptor() {
    return "not a descriptor";
  }
}
