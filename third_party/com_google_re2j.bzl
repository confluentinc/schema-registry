load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_re2j_re2j",
      artifact = "com.google.re2j:re2j:1.6",
      artifact_sha256 = "c8b5c3472d4db594a865b2e47f835d07fb8b1415eeba559dccfb0a6945f033cd",
  )
