load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "io_opencensus_opencensus_api",
      artifact = "io.opencensus:opencensus-api:0.31.1",
      artifact_sha256 = "f1474d47f4b6b001558ad27b952e35eda5cc7146788877fc52938c6eba24b382",
      deps = [
          "@io_grpc_grpc_context"
      ],
  )


  import_external(
      name = "io_opencensus_opencensus_contrib_http_util",
      artifact = "io.opencensus:opencensus-contrib-http-util:0.31.1",
      artifact_sha256 = "3ea995b55a4068be22989b70cc29a4d788c2d328d1d50613a7a9afd13fdd2d0a",
      deps = [
          "@com_google_guava_guava",
          "@io_opencensus_opencensus_api"
      ],
  )
