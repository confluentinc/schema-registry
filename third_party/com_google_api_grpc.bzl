load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_api_grpc_proto_google_common_protos",
      artifact = "com.google.api.grpc:proto-google-common-protos:2.5.1",
      artifact_sha256 = "6bc29d4b2da1cc9953d617b6a7cd4723e3ddc666d1c35dfdb4f11e7ebe3ae556",
      deps = [
          "@com_google_protobuf_protobuf_java"
      ],
  )
