load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_javassist_javassist",
      artifact = "org.javassist:javassist:3.25.0-GA",
      artifact_sha256 = "5d49abd02997134f80041645e9668e1ff97afd69d2c2c55ae9fbd40dc073f97b",
  )
