load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "jakarta_ws_rs_jakarta_ws_rs_api",
      artifact = "jakarta.ws.rs:jakarta.ws.rs-api:2.1.6",
      artifact_sha256 = "4cea299c846c8a6e6470cbfc2f7c391bc29b9caa2f9264ac1064ba91691f4adf",
  )
