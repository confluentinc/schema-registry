/*
 * Copyright 2020 Confluent Inc.
 * Copyright 2015 protobuf-dynamic developers
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.protobuf.dynamic;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;

/**
 * EnumDefinition
 */
public class EnumDefinition {
  // --- public static ---

  public static Builder newBuilder(String enumName) {
    return newBuilder(enumName, null);
  }

  public static Builder newBuilder(String enumName, Boolean allowAlias) {
    return new Builder(enumName, allowAlias);
  }

  // --- public ---

  public String toString() {
    return mEnumType.toString();
  }

  // --- package ---

  EnumDescriptorProto getEnumType() {
    return mEnumType;
  }

  // --- private ---

  private EnumDefinition(EnumDescriptorProto enumType) {
    mEnumType = enumType;
  }

  private EnumDescriptorProto mEnumType;

  /**
   * EnumDefinition.Builder
   */
  public static class Builder {
    // --- public ---

    public Builder addValue(String name, int num) {
      EnumValueDescriptorProto.Builder enumValBuilder = EnumValueDescriptorProto.newBuilder();
      enumValBuilder.setName(name).setNumber(num);
      mEnumTypeBuilder.addValue(enumValBuilder.build());
      return this;
    }

    public EnumDefinition build() {
      return new EnumDefinition(mEnumTypeBuilder.build());
    }

    // --- private ---

    private Builder(String enumName, Boolean allowAlias) {
      mEnumTypeBuilder = EnumDescriptorProto.newBuilder();
      mEnumTypeBuilder.setName(enumName);
      if (allowAlias != null) {
        DescriptorProtos.EnumOptions.Builder optionsBuilder =
            DescriptorProtos.EnumOptions.newBuilder();
        optionsBuilder.setAllowAlias(allowAlias);
        mEnumTypeBuilder.mergeOptions(optionsBuilder.build());
      }
    }

    private EnumDescriptorProto.Builder mEnumTypeBuilder;
  }
}
