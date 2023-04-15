load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "jakarta_annotation_jakarta_annotation_api",
      artifact = "jakarta.annotation:jakarta.annotation-api:1.3.5",
      artifact_sha256 = "85fb03fc054cdf4efca8efd9b6712bbb418e1ab98241c4539c8585bbc23e1b8a",
  )
