/*
 * Copyright 2020 Confluent Inc.
 * Copyright 2015 protobuf-dynamic developers
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

package io.confluent.kafka.schemaregistry.protobuf.dynamic;

import static io.confluent.kafka.schemaregistry.protobuf.dynamic.DynamicSchema.toMeta;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto.EnumReservedRange;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FeatureSet;
import com.google.protobuf.DescriptorProtos.FieldOptions.FeatureSupport;
import io.confluent.kafka.schemaregistry.protobuf.ProtobufSchema.ProtobufMeta;
import io.confluent.protobuf.MetaProto;
import io.confluent.protobuf.MetaProto.Meta;

/**
 * EnumDefinition
 */
public class EnumDefinition {
  // --- public static ---

  public static Builder newBuilder(String enumName) {
    return newBuilder(enumName, null, null, null);
  }

  public static Builder newBuilder(
      String enumName, Boolean allowAlias, Boolean isDeprecated, FeatureSet features) {
    return new Builder(enumName, allowAlias, isDeprecated, features);
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

    public String getName() {
      return mEnumTypeBuilder.getName();
    }

    public Builder addValue(String name, int num) {
      return addValue(name, num, null, null, null, null, null);
    }

    // Note: added
    public Builder addValue(String name, int num, ProtobufMeta meta,
        Boolean isDeprecated, Boolean isDebugRedact,
        FeatureSet features, FeatureSupport featureSupport) {
      EnumValueDescriptorProto.Builder enumValBuilder = EnumValueDescriptorProto.newBuilder();
      enumValBuilder.setName(name).setNumber(num);
      if (isDeprecated != null) {
        DescriptorProtos.EnumValueOptions.Builder optionsBuilder =
                DescriptorProtos.EnumValueOptions.newBuilder();
        optionsBuilder.setDeprecated(isDeprecated);
        enumValBuilder.mergeOptions(optionsBuilder.build());
      }
      if (isDebugRedact != null) {
        DescriptorProtos.EnumValueOptions.Builder optionsBuilder =
            DescriptorProtos.EnumValueOptions.newBuilder();
        optionsBuilder.setDebugRedact(isDebugRedact);
        enumValBuilder.mergeOptions(optionsBuilder.build());
      }
      if (features != null) {
        DescriptorProtos.EnumValueOptions.Builder optionsBuilder =
            DescriptorProtos.EnumValueOptions.newBuilder();
        optionsBuilder.setFeatures(features);
        enumValBuilder.mergeOptions(optionsBuilder.build());
      }
      if (featureSupport != null) {
        DescriptorProtos.EnumValueOptions.Builder optionsBuilder =
            DescriptorProtos.EnumValueOptions.newBuilder();
        optionsBuilder.setFeatureSupport(featureSupport);
        enumValBuilder.mergeOptions(optionsBuilder.build());
      }
      Meta m = toMeta(meta);
      if (m != null) {
        DescriptorProtos.EnumValueOptions.Builder optionsBuilder =
                DescriptorProtos.EnumValueOptions.newBuilder();
        optionsBuilder.setExtension(MetaProto.enumValueMeta, m);
        enumValBuilder.mergeOptions(optionsBuilder.build());
      }
      mEnumTypeBuilder.addValue(enumValBuilder.build());
      return this;
    }

    // Note: added
    public Builder addReservedName(String reservedName) {
      mEnumTypeBuilder.addReservedName(reservedName);
      return this;
    }

    // Note: added
    public Builder addReservedRange(int start, int end) {
      EnumReservedRange.Builder rangeBuilder = EnumReservedRange.newBuilder();
      rangeBuilder.setStart(start).setEnd(end);
      mEnumTypeBuilder.addReservedRange(rangeBuilder.build());
      return this;
    }

    // Note: added
    public Builder setMeta(ProtobufMeta meta) {
      Meta m = toMeta(meta);
      if (m != null) {
        DescriptorProtos.EnumOptions.Builder optionsBuilder =
                DescriptorProtos.EnumOptions.newBuilder();
        optionsBuilder.setExtension(MetaProto.enumMeta, m);
        mEnumTypeBuilder.mergeOptions(optionsBuilder.build());
      }
      return this;
    }

    public EnumDefinition build() {
      return new EnumDefinition(mEnumTypeBuilder.build());
    }

    // --- private ---

    private Builder(
        String enumName, Boolean allowAlias, Boolean isDeprecated, FeatureSet features) {
      mEnumTypeBuilder = EnumDescriptorProto.newBuilder();
      mEnumTypeBuilder.setName(enumName);
      if (allowAlias != null) {
        DescriptorProtos.EnumOptions.Builder optionsBuilder =
            DescriptorProtos.EnumOptions.newBuilder();
        optionsBuilder.setAllowAlias(allowAlias);
        mEnumTypeBuilder.mergeOptions(optionsBuilder.build());
      }
      if (isDeprecated != null) {
        DescriptorProtos.EnumOptions.Builder optionsBuilder =
            DescriptorProtos.EnumOptions.newBuilder();
        optionsBuilder.setDeprecated(isDeprecated);
        mEnumTypeBuilder.mergeOptions(optionsBuilder.build());
      }
      if (features != null) {
        DescriptorProtos.EnumOptions.Builder optionsBuilder =
            DescriptorProtos.EnumOptions.newBuilder();
        optionsBuilder.setFeatures(features);
        mEnumTypeBuilder.mergeOptions(optionsBuilder.build());
      }
    }

    private EnumDescriptorProto.Builder mEnumTypeBuilder;
  }
}
