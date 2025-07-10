/*
 * Copyright 2021 Confluent Inc.
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

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.DescriptorProtos.FeatureSet;
import com.google.protobuf.DescriptorProtos.MethodDescriptorProto;
import com.google.protobuf.DescriptorProtos.MethodOptions.IdempotencyLevel;
import com.google.protobuf.DescriptorProtos.ServiceDescriptorProto;

/**
 * ServiceDefinition
 */
public class ServiceDefinition {
  // --- public static ---

  public static Builder newBuilder(String serviceName) {
    return new Builder(serviceName);
  }

  // --- public ---

  public String toString() {
    return mServiceType.toString();
  }

  // --- package ---

  ServiceDescriptorProto getServiceType() {
    return mServiceType;
  }

  // --- private ---

  private ServiceDefinition(ServiceDescriptorProto serviceType) {
    mServiceType = serviceType;
  }

  private ServiceDescriptorProto mServiceType;

  /**
   * ServiceDefinition.Builder
   */
  public static class Builder {
    // --- public ---

    public void addMethod(
        String name,
        String inputType,
        String outputType,
        Boolean clientStreaming,
        Boolean serverStreaming,
        Boolean isDeprecated,
        IdempotencyLevel idempotencyLevel,
        FeatureSet features
    ) {
      MethodDescriptorProto.Builder methodBuilder = MethodDescriptorProto.newBuilder();
      methodBuilder.setName(name)
          .setInputType(inputType)
          .setOutputType(outputType);
      if (clientStreaming != null) {
        methodBuilder.setClientStreaming(clientStreaming);
      }
      if (serverStreaming != null) {
        methodBuilder.setServerStreaming(serverStreaming);
      }
      if (isDeprecated != null) {
        DescriptorProtos.MethodOptions.Builder optionsBuilder =
            DescriptorProtos.MethodOptions.newBuilder();
        optionsBuilder.setDeprecated(isDeprecated);
        methodBuilder.mergeOptions(optionsBuilder.build());
      }
      if (idempotencyLevel != null) {
        DescriptorProtos.MethodOptions.Builder optionsBuilder =
            DescriptorProtos.MethodOptions.newBuilder();
        optionsBuilder.setIdempotencyLevel(idempotencyLevel);
        methodBuilder.mergeOptions(optionsBuilder.build());
      }
      if (features != null) {
        DescriptorProtos.MethodOptions.Builder optionsBuilder =
            DescriptorProtos.MethodOptions.newBuilder();
        optionsBuilder.setFeatures(features);
        methodBuilder.mergeOptions(optionsBuilder.build());
      }
      mServiceTypeBuilder.addMethod(methodBuilder.build());
    }

    public Builder setDeprecated(boolean isDeprecated) {
      DescriptorProtos.ServiceOptions.Builder optionsBuilder =
          DescriptorProtos.ServiceOptions.newBuilder();
      optionsBuilder.setDeprecated(isDeprecated);
      mServiceTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public Builder setFeatures(FeatureSet features) {
      DescriptorProtos.ServiceOptions.Builder optionsBuilder =
          DescriptorProtos.ServiceOptions.newBuilder();
      optionsBuilder.setFeatures(features);
      mServiceTypeBuilder.mergeOptions(optionsBuilder.build());
      return this;
    }

    public ServiceDefinition build() {
      return new ServiceDefinition(mServiceTypeBuilder.build());
    }

    // --- private ---

    private Builder(String serviceTypeName) {
      mServiceTypeBuilder = ServiceDescriptorProto.newBuilder();
      mServiceTypeBuilder.setName(serviceTypeName);
    }

    private ServiceDescriptorProto.Builder mServiceTypeBuilder;
  }
}