load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "commons_pool_commons_pool",
      artifact = "commons-pool:commons-pool:1.6",
      artifact_sha256 = "46c42b4a38dc6b2db53a9ee5c92c63db103665d56694e2cfce2c95d51a6860cc",
  )
