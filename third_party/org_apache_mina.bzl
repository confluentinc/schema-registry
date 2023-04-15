load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_mina_mina_core",
      artifact = "org.apache.mina:mina-core:2.0.13",
      artifact_sha256 = "951d3b40b6f3e6351e13748ec711d79f25466937b5608838dcf7f765e96ce12b",
      deps = [
          "@org_slf4j_slf4j_api"
      ],
  )
