load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "commons_io_commons_io",
      artifact = "commons-io:commons-io:2.4",
      artifact_sha256 = "cc6a41dc3eaacc9e440a6bd0d2890b20d36b4ee408fe2d67122f328bb6e01581",
  )
