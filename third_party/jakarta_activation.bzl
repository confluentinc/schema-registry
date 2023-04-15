load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "jakarta_activation_jakarta_activation_api",
      artifact = "jakarta.activation:jakarta.activation-api:1.2.1",
      artifact_sha256 = "8b0a0f52fa8b05c5431921a063ed866efaa41dadf2e3a7ee3e1961f2b0d9645b",
  )
