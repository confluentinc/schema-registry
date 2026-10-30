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

package io.confluent.kafka.serializers;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

// Classes that schemas name, each recording whether its static initializer ran
public class SchemaNamedClasses {

  static final Set<String> initialized = ConcurrentHashMap.newKeySet();

  public static class TopLevelRecord {
    static {
      initialized.add("TopLevelRecord");
    }
  }

  public static class UnionBranch {
    static {
      initialized.add("UnionBranch");
    }
  }

  public static class ArrayElement {
    static {
      initialized.add("ArrayElement");
    }
  }

  public static class MapValue {
    static {
      initialized.add("MapValue");
    }
  }

  public static class NestedRecord {
    static {
      initialized.add("NestedRecord");
    }
  }

  public static class NestedEnum {
    static {
      initialized.add("NestedEnum");
    }
  }

  public static class NestedFixed {
    static {
      initialized.add("NestedFixed");
    }
  }

  public static class LatestVersion {
    static {
      initialized.add("LatestVersion");
    }
  }

  public static class SpecificDataChoice {
    static {
      initialized.add("SpecificDataChoice");
    }
  }

  public static class CopyTarget {
    static {
      initialized.add("CopyTarget");
    }
  }
}
