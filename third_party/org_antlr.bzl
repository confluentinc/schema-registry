load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_antlr_antlr4_runtime",
      artifact = "org.antlr:antlr4-runtime:4.11.1",
      artifact_sha256 = "e06c6553c1ccc14d36052ec4b0fc6f13b808cf957b5b1dc3f61bf401996ada59",
  )
