load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "jakarta_el_jakarta_el_api",
      artifact = "jakarta.el:jakarta.el-api:3.0.3",
  )
