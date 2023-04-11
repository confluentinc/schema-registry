load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "javax_validation_validation_api",
      artifact = "javax.validation:validation-api:2.0.1.Final",
      artifact_sha256 = "9873b46df1833c9ee8f5bc1ff6853375115dadd8897bcb5a0dffb5848835ee6c",
  )
