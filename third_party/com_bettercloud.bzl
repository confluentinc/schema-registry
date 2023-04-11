load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_bettercloud_vault_java_driver",
      artifact = "com.bettercloud:vault-java-driver:5.1.0",
      artifact_sha256 = "b5ef2f95b6acd5faa4ecb99d342df98ffc2f494f3397892d904e2a4f5d96d0ad",
  )
