load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "io_grpc_grpc_context",
      artifact = "io.grpc:grpc-context:1.27.2",
      artifact_sha256 = "bcbf9055dff453fd6508bd7cca2a0aa2d5f059a9c94beed1f5fda1dc015607b8",
  )
