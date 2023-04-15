load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_checkerframework_checker_qual",
      artifact = "org.checkerframework:checker-qual:3.8.0",
      artifact_sha256 = "c88c2e6a5fdaeb9f26fcf879264042de8a9ee9d376e2477838feaabcfa44dda6",
  )
