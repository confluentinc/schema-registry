load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_fasterxml_woodstox_woodstox_core",
      artifact = "com.fasterxml.woodstox:woodstox-core:6.4.0",
      artifact_sha256 = "d6eb9f2f40049a7a808baf11ffba0737648e62ff52fde9271d808e5d57a27279",
      deps = [
          "@org_codehaus_woodstox_stax2_api"
      ],
  )
