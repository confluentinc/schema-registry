#!/bin/bash

# Exit on any error
set -e
set -x

echo "Building MSK Connect Iceberg Sink Connector custom plugin package..."

# Extract the existing Tabular Iceberg connector package
echo "Extracting Tabular Iceberg connector package..."
CURRENT_DATE=$(date +%Y%m%d)
unzip tabular-iceberg-kafka-connect-0.6.19.zip -x "__MACOSX/*"
WORK_DIR="tabular-iceberg-kafka-connect-0.6.19"

cd "$WORK_DIR"

echo "Downloading additional required JARs that are missing from tabular connector..."

# Confluent dependencies (version 7.6.0) - EXCLUDING kafka-avro-serializer (we'll use our custom one)
echo "Downloading Confluent 7.6.0 dependencies..."
VER=7.6.0
BASE=https://packages.confluent.io/maven/io/confluent

for jar in kafka-connect-avro-converter \
           kafka-connect-avro-data \
           kafka-schema-serializer \
           kafka-schema-converter \
           kafka-schema-registry-client \
           kafka-schema-registry \
           common-config \
           common-utils
do
  echo "Downloading standard Confluent JAR: $jar-$VER.jar"
  wget -O lib/$jar-$VER.jar $BASE/$jar/$VER/$jar-$VER.jar
done

# Download our CUSTOM kafka-avro-serializer with HeaderBasedAvroDeserializer
echo "=========================================="
echo "🚀 Downloading CUSTOM Cogent kafka-avro-serializer with HeaderBasedAvroDeserializer..."
echo "   This JAR includes header-based subject routing for DLQ functionality"
echo "=========================================="
COGENT_TAG=v7.6.0-cogent-1
COGENT_BASE=https://github.com/cogent-security/schema-registry/releases/download/$COGENT_TAG
wget -O lib/kafka-avro-serializer-$VER.jar $COGENT_BASE/kafka-avro-serializer-$VER.jar

# Verify the custom JAR was downloaded
if [ -f "lib/kafka-avro-serializer-$VER.jar" ]; then
    CUSTOM_JAR_SIZE=$(stat -f%z lib/kafka-avro-serializer-$VER.jar 2>/dev/null || stat -c%s lib/kafka-avro-serializer-$VER.jar 2>/dev/null)
    echo "✅ Custom kafka-avro-serializer downloaded successfully ($CUSTOM_JAR_SIZE bytes)"
    echo "   This JAR contains com.cogent.kafka.serializers.HeaderBasedAvroDeserializer"
else
    echo "❌ Failed to download custom kafka-avro-serializer JAR"
    exit 1
fi

# Apache Kafka Connect dependencies
echo "Downloading Apache Kafka Connect dependencies..."
KAFKA_VER=3.6.0
KAFKA_BASE=https://repo1.maven.org/maven2/org/apache/kafka
for jar in connect-api connect-transforms
do
  echo "Downloading Kafka Connect JAR: $jar-$KAFKA_VER.jar"
  wget -O lib/$jar-$KAFKA_VER.jar $KAFKA_BASE/$jar/$KAFKA_VER/$jar-$KAFKA_VER.jar
done

# Create the final zip file in the original directory
echo "Creating final plugin package..."
cd ..
ZIP_FILE="iceberg-kafka-connector-cogent-${CURRENT_DATE}.zip"
zip -r "$ZIP_FILE" "$WORK_DIR/"

# Cleanup
# rm -rf "$WORK_DIR"

echo "=========================================="
echo "🎉 Plugin package created successfully: $ZIP_FILE"
echo "   This package includes Cogent's HeaderBasedAvroDeserializer for header-based subject routing!"
echo "   Configure your connector with:"
echo "   value.converter.value.deserializer.class=com.cogent.kafka.serializers.HeaderBasedAvroDeserializer"
echo "=========================================="

# Upload to S3 bucket
echo "Uploading plugin package to S3..."
aws s3 cp "$ZIP_FILE" s3://cgnt-artifacts-us-west-2/
echo "Upload complete - your custom Cogent connector is ready for deployment!" 