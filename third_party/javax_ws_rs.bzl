load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "javax_ws_rs_javax_ws_rs_api",
      artifact = "javax.ws.rs:javax.ws.rs-api:2.1.1",
      artifact_sha256 = "2c309eb2c9455ffee9da8518c70a3b6d46be2a269b2e2a101c806a537efe79a4",
  )
