// Generated by the protocol buffer compiler.  DO NOT EDIT!
// NO CHECKED-IN PROTOBUF GENCODE
// source: TimestampValue.proto
// Protobuf Java Version: 4.29.3

package io.confluent.kafka.serializers.protobuf.test;

public final class TimestampValueOuterClass {
  private TimestampValueOuterClass() {}
  static {
    com.google.protobuf.RuntimeVersion.validateProtobufGencodeVersion(
      com.google.protobuf.RuntimeVersion.RuntimeDomain.PUBLIC,
      /* major= */ 4,
      /* minor= */ 29,
      /* patch= */ 3,
      /* suffix= */ "",
      TimestampValueOuterClass.class.getName());
  }
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface TimestampValueOrBuilder extends
      // @@protoc_insertion_point(interface_extends:TimestampValue)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <pre>
     * The bytes value.
     * </pre>
     *
     * <code>.google.protobuf.Timestamp value = 1;</code>
     * @return Whether the value field is set.
     */
    boolean hasValue();
    /**
     * <pre>
     * The bytes value.
     * </pre>
     *
     * <code>.google.protobuf.Timestamp value = 1;</code>
     * @return The value.
     */
    com.google.protobuf.Timestamp getValue();
    /**
     * <pre>
     * The bytes value.
     * </pre>
     *
     * <code>.google.protobuf.Timestamp value = 1;</code>
     */
    com.google.protobuf.TimestampOrBuilder getValueOrBuilder();
  }
  /**
   * <pre>
   * Wrapper message for `Timestamp`.
   * </pre>
   *
   * Protobuf type {@code TimestampValue}
   */
  public static final class TimestampValue extends
      com.google.protobuf.GeneratedMessage implements
      // @@protoc_insertion_point(message_implements:TimestampValue)
      TimestampValueOrBuilder {
  private static final long serialVersionUID = 0L;
    static {
      com.google.protobuf.RuntimeVersion.validateProtobufGencodeVersion(
        com.google.protobuf.RuntimeVersion.RuntimeDomain.PUBLIC,
        /* major= */ 4,
        /* minor= */ 29,
        /* patch= */ 3,
        /* suffix= */ "",
        TimestampValue.class.getName());
    }
    // Use TimestampValue.newBuilder() to construct.
    private TimestampValue(com.google.protobuf.GeneratedMessage.Builder<?> builder) {
      super(builder);
    }
    private TimestampValue() {
    }

    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.internal_static_TimestampValue_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.internal_static_TimestampValue_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue.class, io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue.Builder.class);
    }

    private int bitField0_;
    public static final int VALUE_FIELD_NUMBER = 1;
    private com.google.protobuf.Timestamp value_;
    /**
     * <pre>
     * The bytes value.
     * </pre>
     *
     * <code>.google.protobuf.Timestamp value = 1;</code>
     * @return Whether the value field is set.
     */
    @java.lang.Override
    public boolean hasValue() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <pre>
     * The bytes value.
     * </pre>
     *
     * <code>.google.protobuf.Timestamp value = 1;</code>
     * @return The value.
     */
    @java.lang.Override
    public com.google.protobuf.Timestamp getValue() {
      return value_ == null ? com.google.protobuf.Timestamp.getDefaultInstance() : value_;
    }
    /**
     * <pre>
     * The bytes value.
     * </pre>
     *
     * <code>.google.protobuf.Timestamp value = 1;</code>
     */
    @java.lang.Override
    public com.google.protobuf.TimestampOrBuilder getValueOrBuilder() {
      return value_ == null ? com.google.protobuf.Timestamp.getDefaultInstance() : value_;
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeMessage(1, getValue());
      }
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeMessageSize(1, getValue());
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue)) {
        return super.equals(obj);
      }
      io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue other = (io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue) obj;

      if (hasValue() != other.hasValue()) return false;
      if (hasValue()) {
        if (!getValue()
            .equals(other.getValue())) return false;
      }
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasValue()) {
        hash = (37 * hash) + VALUE_FIELD_NUMBER;
        hash = (53 * hash) + getValue().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseWithIOException(PARSER, input);
    }
    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseDelimitedWithIOException(PARSER, input);
    }

    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseWithIOException(PARSER, input);
    }
    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessage
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessage.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * <pre>
     * Wrapper message for `Timestamp`.
     * </pre>
     *
     * Protobuf type {@code TimestampValue}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessage.Builder<Builder> implements
        // @@protoc_insertion_point(builder_implements:TimestampValue)
        io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValueOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.internal_static_TimestampValue_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessage.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.internal_static_TimestampValue_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue.class, io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue.Builder.class);
      }

      // Construct using io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue.newBuilder()
      private Builder() {
        maybeForceBuilderInitialization();
      }

      private Builder(
          com.google.protobuf.GeneratedMessage.BuilderParent parent) {
        super(parent);
        maybeForceBuilderInitialization();
      }
      private void maybeForceBuilderInitialization() {
        if (com.google.protobuf.GeneratedMessage
                .alwaysUseFieldBuilders) {
          getValueFieldBuilder();
        }
      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        value_ = null;
        if (valueBuilder_ != null) {
          valueBuilder_.dispose();
          valueBuilder_ = null;
        }
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.internal_static_TimestampValue_descriptor;
      }

      @java.lang.Override
      public io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue getDefaultInstanceForType() {
        return io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue.getDefaultInstance();
      }

      @java.lang.Override
      public io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue build() {
        io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue buildPartial() {
        io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue result = new io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue(this);
        if (bitField0_ != 0) { buildPartial0(result); }
        onBuilt();
        return result;
      }

      private void buildPartial0(io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.value_ = valueBuilder_ == null
              ? value_
              : valueBuilder_.build();
          to_bitField0_ |= 0x00000001;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue) {
          return mergeFrom((io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue other) {
        if (other == io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue.getDefaultInstance()) return this;
        if (other.hasValue()) {
          mergeValue(other.getValue());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new java.lang.NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 10: {
                input.readMessage(
                    getValueFieldBuilder().getBuilder(),
                    extensionRegistry);
                bitField0_ |= 0x00000001;
                break;
              } // case 10
              default: {
                if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                  done = true; // was an endgroup tag
                }
                break;
              } // default:
            } // switch (tag)
          } // while (!done)
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }
      private int bitField0_;

      private com.google.protobuf.Timestamp value_;
      private com.google.protobuf.SingleFieldBuilder<
          com.google.protobuf.Timestamp, com.google.protobuf.Timestamp.Builder, com.google.protobuf.TimestampOrBuilder> valueBuilder_;
      /**
       * <pre>
       * The bytes value.
       * </pre>
       *
       * <code>.google.protobuf.Timestamp value = 1;</code>
       * @return Whether the value field is set.
       */
      public boolean hasValue() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <pre>
       * The bytes value.
       * </pre>
       *
       * <code>.google.protobuf.Timestamp value = 1;</code>
       * @return The value.
       */
      public com.google.protobuf.Timestamp getValue() {
        if (valueBuilder_ == null) {
          return value_ == null ? com.google.protobuf.Timestamp.getDefaultInstance() : value_;
        } else {
          return valueBuilder_.getMessage();
        }
      }
      /**
       * <pre>
       * The bytes value.
       * </pre>
       *
       * <code>.google.protobuf.Timestamp value = 1;</code>
       */
      public Builder setValue(com.google.protobuf.Timestamp value) {
        if (valueBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          value_ = value;
        } else {
          valueBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * The bytes value.
       * </pre>
       *
       * <code>.google.protobuf.Timestamp value = 1;</code>
       */
      public Builder setValue(
          com.google.protobuf.Timestamp.Builder builderForValue) {
        if (valueBuilder_ == null) {
          value_ = builderForValue.build();
        } else {
          valueBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /**
       * <pre>
       * The bytes value.
       * </pre>
       *
       * <code>.google.protobuf.Timestamp value = 1;</code>
       */
      public Builder mergeValue(com.google.protobuf.Timestamp value) {
        if (valueBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0) &&
            value_ != null &&
            value_ != com.google.protobuf.Timestamp.getDefaultInstance()) {
            getValueBuilder().mergeFrom(value);
          } else {
            value_ = value;
          }
        } else {
          valueBuilder_.mergeFrom(value);
        }
        if (value_ != null) {
          bitField0_ |= 0x00000001;
          onChanged();
        }
        return this;
      }
      /**
       * <pre>
       * The bytes value.
       * </pre>
       *
       * <code>.google.protobuf.Timestamp value = 1;</code>
       */
      public Builder clearValue() {
        bitField0_ = (bitField0_ & ~0x00000001);
        value_ = null;
        if (valueBuilder_ != null) {
          valueBuilder_.dispose();
          valueBuilder_ = null;
        }
        onChanged();
        return this;
      }
      /**
       * <pre>
       * The bytes value.
       * </pre>
       *
       * <code>.google.protobuf.Timestamp value = 1;</code>
       */
      public com.google.protobuf.Timestamp.Builder getValueBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getValueFieldBuilder().getBuilder();
      }
      /**
       * <pre>
       * The bytes value.
       * </pre>
       *
       * <code>.google.protobuf.Timestamp value = 1;</code>
       */
      public com.google.protobuf.TimestampOrBuilder getValueOrBuilder() {
        if (valueBuilder_ != null) {
          return valueBuilder_.getMessageOrBuilder();
        } else {
          return value_ == null ?
              com.google.protobuf.Timestamp.getDefaultInstance() : value_;
        }
      }
      /**
       * <pre>
       * The bytes value.
       * </pre>
       *
       * <code>.google.protobuf.Timestamp value = 1;</code>
       */
      private com.google.protobuf.SingleFieldBuilder<
          com.google.protobuf.Timestamp, com.google.protobuf.Timestamp.Builder, com.google.protobuf.TimestampOrBuilder> 
          getValueFieldBuilder() {
        if (valueBuilder_ == null) {
          valueBuilder_ = new com.google.protobuf.SingleFieldBuilder<
              com.google.protobuf.Timestamp, com.google.protobuf.Timestamp.Builder, com.google.protobuf.TimestampOrBuilder>(
                  getValue(),
                  getParentForChildren(),
                  isClean());
          value_ = null;
        }
        return valueBuilder_;
      }

      // @@protoc_insertion_point(builder_scope:TimestampValue)
    }

    // @@protoc_insertion_point(class_scope:TimestampValue)
    private static final io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue();
    }

    public static io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<TimestampValue>
        PARSER = new com.google.protobuf.AbstractParser<TimestampValue>() {
      @java.lang.Override
      public TimestampValue parsePartialFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws com.google.protobuf.InvalidProtocolBufferException {
        Builder builder = newBuilder();
        try {
          builder.mergeFrom(input, extensionRegistry);
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw e.setUnfinishedMessage(builder.buildPartial());
        } catch (com.google.protobuf.UninitializedMessageException e) {
          throw e.asInvalidProtocolBufferException().setUnfinishedMessage(builder.buildPartial());
        } catch (java.io.IOException e) {
          throw new com.google.protobuf.InvalidProtocolBufferException(e)
              .setUnfinishedMessage(builder.buildPartial());
        }
        return builder.buildPartial();
      }
    };

    public static com.google.protobuf.Parser<TimestampValue> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<TimestampValue> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public io.confluent.kafka.serializers.protobuf.test.TimestampValueOuterClass.TimestampValue getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_TimestampValue_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessage.FieldAccessorTable
      internal_static_TimestampValue_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\024TimestampValue.proto\032\037google/protobuf/" +
      "timestamp.proto\";\n\016TimestampValue\022)\n\005val" +
      "ue\030\001 \001(\0132\032.google.protobuf.TimestampB.\n," +
      "io.confluent.kafka.serializers.protobuf." +
      "testb\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          com.google.protobuf.TimestampProto.getDescriptor(),
        });
    internal_static_TimestampValue_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_TimestampValue_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessage.FieldAccessorTable(
        internal_static_TimestampValue_descriptor,
        new java.lang.String[] { "Value", });
    descriptor.resolveAllFeaturesImmutable();
    com.google.protobuf.TimestampProto.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
