bazel run --define "maven_repo=file://$HOME/.m2/repository" //avro-serializer:avro-serializer.publish
bazel run --define "maven_repo=file://$HOME/.m2/repository" //client:client.publish
bazel run --define "maven_repo=file://$HOME/.m2/repository" //json-schema-provider:json-schema-provider.publish
bazel run --define "maven_repo=file://$HOME/.m2/repository" //protobuf-provider:protobuf-provider.publish
bazel run --define "maven_repo=file://$HOME/.m2/repository" //protobuf-serializer:protobuf-serializer.publish
