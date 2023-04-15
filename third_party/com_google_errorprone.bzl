load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_errorprone_error_prone_annotations",
      artifact = "com.google.errorprone:error_prone_annotations:2.5.1",
      artifact_sha256 = "ff80626baaf12a09342befd4e84cba9d50662f5fcd7f7a9b3490a6b7cf87e66c",
  )
