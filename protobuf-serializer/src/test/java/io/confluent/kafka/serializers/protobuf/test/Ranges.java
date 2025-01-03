// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Ranges.proto

// Protobuf Java Version: 3.25.5
package io.confluent.kafka.serializers.protobuf.test;

public final class Ranges {
  private Ranges() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  public interface FooBarOrBuilder extends
      // @@protoc_insertion_point(interface_extends:io.confluent.kafka.serializers.protobuf.test.FooBar)
      com.google.protobuf.GeneratedMessageV3.
          ExtendableMessageOrBuilder<FooBar> {

    /**
     * <code>optional int32 foo = 1;</code>
     * @return Whether the foo field is set.
     */
    boolean hasFoo();
    /**
     * <code>optional int32 foo = 1;</code>
     * @return The foo.
     */
    int getFoo();

    /**
     * <code>optional string bar = 2;</code>
     * @return Whether the bar field is set.
     */
    boolean hasBar();
    /**
     * <code>optional string bar = 2;</code>
     * @return The bar.
     */
    java.lang.String getBar();
    /**
     * <code>optional string bar = 2;</code>
     * @return The bytes for bar.
     */
    com.google.protobuf.ByteString
        getBarBytes();
  }
  /**
   * Protobuf type {@code io.confluent.kafka.serializers.protobuf.test.FooBar}
   */
  public static final class FooBar extends
      com.google.protobuf.GeneratedMessageV3.ExtendableMessage<
        FooBar> implements
      // @@protoc_insertion_point(message_implements:io.confluent.kafka.serializers.protobuf.test.FooBar)
      FooBarOrBuilder {
  private static final long serialVersionUID = 0L;
    // Use FooBar.newBuilder() to construct.
    private FooBar(com.google.protobuf.GeneratedMessageV3.ExtendableBuilder<io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar, ?> builder) {
      super(builder);
    }
    private FooBar() {
      bar_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(
        UnusedPrivateParameter unused) {
      return new FooBar();
    }

    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return io.confluent.kafka.serializers.protobuf.test.Ranges.internal_static_io_confluent_kafka_serializers_protobuf_test_FooBar_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return io.confluent.kafka.serializers.protobuf.test.Ranges.internal_static_io_confluent_kafka_serializers_protobuf_test_FooBar_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar.class, io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar.Builder.class);
    }

    /**
     * Protobuf enum {@code io.confluent.kafka.serializers.protobuf.test.FooBar.FooBarBazEnum}
     */
    public enum FooBarBazEnum
        implements com.google.protobuf.ProtocolMessageEnum {
      /**
       * <code>NONE = 0;</code>
       */
      NONE(0),
      /**
       * <code>FOO = 1;</code>
       */
      FOO(1),
      /**
       * <code>BAR = 2;</code>
       */
      BAR(2),
      /**
       * <code>BAZ = 3;</code>
       */
      BAZ(3),
      ;

      /**
       * <code>NONE = 0;</code>
       */
      public static final int NONE_VALUE = 0;
      /**
       * <code>FOO = 1;</code>
       */
      public static final int FOO_VALUE = 1;
      /**
       * <code>BAR = 2;</code>
       */
      public static final int BAR_VALUE = 2;
      /**
       * <code>BAZ = 3;</code>
       */
      public static final int BAZ_VALUE = 3;


      public final int getNumber() {
        return value;
      }

      /**
       * @param value The numeric wire value of the corresponding enum entry.
       * @return The enum associated with the given numeric wire value.
       * @deprecated Use {@link #forNumber(int)} instead.
       */
      @java.lang.Deprecated
      public static FooBarBazEnum valueOf(int value) {
        return forNumber(value);
      }

      /**
       * @param value The numeric wire value of the corresponding enum entry.
       * @return The enum associated with the given numeric wire value.
       */
      public static FooBarBazEnum forNumber(int value) {
        switch (value) {
          case 0: return NONE;
          case 1: return FOO;
          case 2: return BAR;
          case 3: return BAZ;
          default: return null;
        }
      }

      public static com.google.protobuf.Internal.EnumLiteMap<FooBarBazEnum>
          internalGetValueMap() {
        return internalValueMap;
      }
      private static final com.google.protobuf.Internal.EnumLiteMap<
          FooBarBazEnum> internalValueMap =
            new com.google.protobuf.Internal.EnumLiteMap<FooBarBazEnum>() {
              public FooBarBazEnum findValueByNumber(int number) {
                return FooBarBazEnum.forNumber(number);
              }
            };

      public final com.google.protobuf.Descriptors.EnumValueDescriptor
          getValueDescriptor() {
        return getDescriptor().getValues().get(ordinal());
      }
      public final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptorForType() {
        return getDescriptor();
      }
      public static final com.google.protobuf.Descriptors.EnumDescriptor
          getDescriptor() {
        return io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar.getDescriptor().getEnumTypes().get(0);
      }

      private static final FooBarBazEnum[] VALUES = values();

      public static FooBarBazEnum valueOf(
          com.google.protobuf.Descriptors.EnumValueDescriptor desc) {
        if (desc.getType() != getDescriptor()) {
          throw new java.lang.IllegalArgumentException(
            "EnumValueDescriptor is not for this type.");
        }
        return VALUES[desc.getIndex()];
      }

      private final int value;

      private FooBarBazEnum(int value) {
        this.value = value;
      }

      // @@protoc_insertion_point(enum_scope:io.confluent.kafka.serializers.protobuf.test.FooBar.FooBarBazEnum)
    }

    private int bitField0_;
    public static final int FOO_FIELD_NUMBER = 1;
    private int foo_ = 0;
    /**
     * <code>optional int32 foo = 1;</code>
     * @return Whether the foo field is set.
     */
    @java.lang.Override
    public boolean hasFoo() {
      return ((bitField0_ & 0x00000001) != 0);
    }
    /**
     * <code>optional int32 foo = 1;</code>
     * @return The foo.
     */
    @java.lang.Override
    public int getFoo() {
      return foo_;
    }

    public static final int BAR_FIELD_NUMBER = 2;
    @SuppressWarnings("serial")
    private volatile java.lang.Object bar_ = "";
    /**
     * <code>optional string bar = 2;</code>
     * @return Whether the bar field is set.
     */
    @java.lang.Override
    public boolean hasBar() {
      return ((bitField0_ & 0x00000002) != 0);
    }
    /**
     * <code>optional string bar = 2;</code>
     * @return The bar.
     */
    @java.lang.Override
    public java.lang.String getBar() {
      java.lang.Object ref = bar_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = 
            (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        if (bs.isValidUtf8()) {
          bar_ = s;
        }
        return s;
      }
    }
    /**
     * <code>optional string bar = 2;</code>
     * @return The bytes for bar.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString
        getBarBytes() {
      java.lang.Object ref = bar_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (java.lang.String) ref);
        bar_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    private byte memoizedIsInitialized = -1;
    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      if (!extensionsAreInitialized()) {
        memoizedIsInitialized = 0;
        return false;
      }
      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output)
                        throws java.io.IOException {
      com.google.protobuf.GeneratedMessageV3
        .ExtendableMessage<io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar>.ExtensionWriter
          extensionWriter = newExtensionWriter();
      if (((bitField0_ & 0x00000001) != 0)) {
        output.writeInt32(1, foo_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 2, bar_);
      }
      extensionWriter.writeUntil(2001, output);
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (((bitField0_ & 0x00000001) != 0)) {
        size += com.google.protobuf.CodedOutputStream
          .computeInt32Size(1, foo_);
      }
      if (((bitField0_ & 0x00000002) != 0)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, bar_);
      }
      size += extensionsSerializedSize();
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
       return true;
      }
      if (!(obj instanceof io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar)) {
        return super.equals(obj);
      }
      io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar other = (io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar) obj;

      if (hasFoo() != other.hasFoo()) return false;
      if (hasFoo()) {
        if (getFoo()
            != other.getFoo()) return false;
      }
      if (hasBar() != other.hasBar()) return false;
      if (hasBar()) {
        if (!getBar()
            .equals(other.getBar())) return false;
      }
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      if (!getExtensionFields().equals(other.getExtensionFields()))
        return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      if (hasFoo()) {
        hash = (37 * hash) + FOO_FIELD_NUMBER;
        hash = (53 * hash) + getFoo();
      }
      if (hasBar()) {
        hash = (37 * hash) + BAR_FIELD_NUMBER;
        hash = (53 * hash) + getBar().hashCode();
      }
      hash = hashFields(hash, getExtensionFields());
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseFrom(
        java.nio.ByteBuffer data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseFrom(
        java.nio.ByteBuffer data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }
    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseFrom(
        byte[] data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }
    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseDelimitedFrom(java.io.InputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input);
    }

    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseDelimitedFrom(
        java.io.InputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
    }
    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseFrom(
        com.google.protobuf.CodedInputStream input)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input);
    }
    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3
          .parseWithIOException(PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() { return newBuilder(); }
    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }
    public static Builder newBuilder(io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }
    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE
          ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     * Protobuf type {@code io.confluent.kafka.serializers.protobuf.test.FooBar}
     */
    public static final class Builder extends
        com.google.protobuf.GeneratedMessageV3.ExtendableBuilder<
          io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar, Builder> implements
        // @@protoc_insertion_point(builder_implements:io.confluent.kafka.serializers.protobuf.test.FooBar)
        io.confluent.kafka.serializers.protobuf.test.Ranges.FooBarOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor
          getDescriptor() {
        return io.confluent.kafka.serializers.protobuf.test.Ranges.internal_static_io_confluent_kafka_serializers_protobuf_test_FooBar_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return io.confluent.kafka.serializers.protobuf.test.Ranges.internal_static_io_confluent_kafka_serializers_protobuf_test_FooBar_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar.class, io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar.Builder.class);
      }

      // Construct using io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar.newBuilder()
      private Builder() {

      }

      private Builder(
          com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);

      }
      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        foo_ = 0;
        bar_ = "";
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor
          getDescriptorForType() {
        return io.confluent.kafka.serializers.protobuf.test.Ranges.internal_static_io_confluent_kafka_serializers_protobuf_test_FooBar_descriptor;
      }

      @java.lang.Override
      public io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar getDefaultInstanceForType() {
        return io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar.getDefaultInstance();
      }

      @java.lang.Override
      public io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar build() {
        io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar buildPartial() {
        io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar result = new io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar(this);
        if (bitField0_ != 0) { buildPartial0(result); }
        onBuilt();
        return result;
      }

      private void buildPartial0(io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar result) {
        int from_bitField0_ = bitField0_;
        int to_bitField0_ = 0;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.foo_ = foo_;
          to_bitField0_ |= 0x00000001;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.bar_ = bar_;
          to_bitField0_ |= 0x00000002;
        }
        result.bitField0_ |= to_bitField0_;
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }
      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.setField(field, value);
      }
      @java.lang.Override
      public Builder clearField(
          com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }
      @java.lang.Override
      public Builder clearOneof(
          com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }
      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index, java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }
      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }
      @java.lang.Override
      public <Type> Builder setExtension(
          com.google.protobuf.GeneratedMessage.GeneratedExtension<
              io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar, Type> extension,
          Type value) {
        return super.setExtension(extension, value);
      }
      @java.lang.Override
      public <Type> Builder setExtension(
          com.google.protobuf.GeneratedMessage.GeneratedExtension<
              io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar, java.util.List<Type>> extension,
          int index, Type value) {
        return super.setExtension(extension, index, value);
      }
      @java.lang.Override
      public <Type> Builder addExtension(
          com.google.protobuf.GeneratedMessage.GeneratedExtension<
              io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar, java.util.List<Type>> extension,
          Type value) {
        return super.addExtension(extension, value);
      }
      @java.lang.Override
      public <T> Builder clearExtension(
          com.google.protobuf.GeneratedMessage.GeneratedExtension<
              io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar, T> extension) {
        return super.clearExtension(extension);
      }
      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar) {
          return mergeFrom((io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar)other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar other) {
        if (other == io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar.getDefaultInstance()) return this;
        if (other.hasFoo()) {
          setFoo(other.getFoo());
        }
        if (other.hasBar()) {
          bar_ = other.bar_;
          bitField0_ |= 0x00000002;
          onChanged();
        }
        this.mergeExtensionFields(other);
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        if (!extensionsAreInitialized()) {
          return false;
        }
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
              case 8: {
                foo_ = input.readInt32();
                bitField0_ |= 0x00000001;
                break;
              } // case 8
              case 18: {
                bar_ = input.readBytes();
                bitField0_ |= 0x00000002;
                break;
              } // case 18
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

      private int foo_ ;
      /**
       * <code>optional int32 foo = 1;</code>
       * @return Whether the foo field is set.
       */
      @java.lang.Override
      public boolean hasFoo() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>optional int32 foo = 1;</code>
       * @return The foo.
       */
      @java.lang.Override
      public int getFoo() {
        return foo_;
      }
      /**
       * <code>optional int32 foo = 1;</code>
       * @param value The foo to set.
       * @return This builder for chaining.
       */
      public Builder setFoo(int value) {

        foo_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /**
       * <code>optional int32 foo = 1;</code>
       * @return This builder for chaining.
       */
      public Builder clearFoo() {
        bitField0_ = (bitField0_ & ~0x00000001);
        foo_ = 0;
        onChanged();
        return this;
      }

      private java.lang.Object bar_ = "";
      /**
       * <code>optional string bar = 2;</code>
       * @return Whether the bar field is set.
       */
      public boolean hasBar() {
        return ((bitField0_ & 0x00000002) != 0);
      }
      /**
       * <code>optional string bar = 2;</code>
       * @return The bar.
       */
      public java.lang.String getBar() {
        java.lang.Object ref = bar_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs =
              (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          if (bs.isValidUtf8()) {
            bar_ = s;
          }
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>optional string bar = 2;</code>
       * @return The bytes for bar.
       */
      public com.google.protobuf.ByteString
          getBarBytes() {
        java.lang.Object ref = bar_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b = 
              com.google.protobuf.ByteString.copyFromUtf8(
                  (java.lang.String) ref);
          bar_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>optional string bar = 2;</code>
       * @param value The bar to set.
       * @return This builder for chaining.
       */
      public Builder setBar(
          java.lang.String value) {
        if (value == null) { throw new NullPointerException(); }
        bar_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }
      /**
       * <code>optional string bar = 2;</code>
       * @return This builder for chaining.
       */
      public Builder clearBar() {
        bar_ = getDefaultInstance().getBar();
        bitField0_ = (bitField0_ & ~0x00000002);
        onChanged();
        return this;
      }
      /**
       * <code>optional string bar = 2;</code>
       * @param value The bytes for bar to set.
       * @return This builder for chaining.
       */
      public Builder setBarBytes(
          com.google.protobuf.ByteString value) {
        if (value == null) { throw new NullPointerException(); }
        bar_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }
      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }


      // @@protoc_insertion_point(builder_scope:io.confluent.kafka.serializers.protobuf.test.FooBar)
    }

    // @@protoc_insertion_point(class_scope:io.confluent.kafka.serializers.protobuf.test.FooBar)
    private static final io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar DEFAULT_INSTANCE;
    static {
      DEFAULT_INSTANCE = new io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar();
    }

    public static io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    @java.lang.Deprecated public static final com.google.protobuf.Parser<FooBar>
        PARSER = new com.google.protobuf.AbstractParser<FooBar>() {
      @java.lang.Override
      public FooBar parsePartialFrom(
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

    public static com.google.protobuf.Parser<FooBar> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<FooBar> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public io.confluent.kafka.serializers.protobuf.test.Ranges.FooBar getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }

  }

  private static final com.google.protobuf.Descriptors.Descriptor
    internal_static_io_confluent_kafka_serializers_protobuf_test_FooBar_descriptor;
  private static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_io_confluent_kafka_serializers_protobuf_test_FooBar_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\014Ranges.proto\022,io.confluent.kafka.seria" +
      "lizers.protobuf.test\032 google/protobuf/de" +
      "scriptor.proto\"\240\001\n\006FooBar\022\013\n\003foo\030\001 \001(\005\022\013" +
      "\n\003bar\030\002 \001(\t\"K\n\rFooBarBazEnum\022\010\n\004NONE\020\000\022\007" +
      "\n\003FOO\020\001\022\007\n\003BAR\020\002\022\007\n\003BAZ\020\003\"\005\010d\020\310\001\"\006\010\350\007\020\351\007" +
      "\"\006\010\320\017\020\320\017*\005\010d\020\311\001*\006\010\350\007\020\352\007*\006\010\320\017\020\321\017J\006\010\210\'\020\361.J" +
      "\006\010\220N\020\222NJ\010\010\240\234\001\020\241\234\001B.\n,io.confluent.kafka." +
      "serializers.protobuf.test"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
          com.google.protobuf.DescriptorProtos.getDescriptor(),
        });
    internal_static_io_confluent_kafka_serializers_protobuf_test_FooBar_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_io_confluent_kafka_serializers_protobuf_test_FooBar_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_io_confluent_kafka_serializers_protobuf_test_FooBar_descriptor,
        new java.lang.String[] { "Foo", "Bar", });
    com.google.protobuf.DescriptorProtos.getDescriptor();
  }

  // @@protoc_insertion_point(outer_class_scope)
}
