load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_reflections_reflections",
      artifact = "org.reflections:reflections:0.9.12",
      artifact_sha256 = "d168f58d32f2ae7ac5a8d5d9092adeee526c604b41125dcb45eea877960a99cf",
      runtime_deps = [
          "@org_javassist_javassist"
      ],
  )
