load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "log4j_log4j",
      artifact = "log4j:log4j:1.2.17",
  )
