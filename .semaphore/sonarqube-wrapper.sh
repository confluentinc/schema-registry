set -e

modules=("avro-converter" "avro-data" "avro-serde" "avro-serializer" "benchmark" "client" "client-console-scripts" "core" "json-schema-converter" "json-schema-provider" "json-schema-serde" "json-schema-serializer" "json-serializer" "maven-plugin" "protobuf-converter" "protobuf-provider" "protobuf-serde" "protobuf-serializer" "protobuf-types" "schema-serializer")

checkout
sem-version java 11
artifact pull workflow target
for dir in ${modules[@]}; do
  echo "Scanning $dir"
  emit-sonarqube-data --run_only_sonar_scan
done
