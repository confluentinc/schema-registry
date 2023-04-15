load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "androidx_annotation_annotation",
      artifact = "androidx.annotation:annotation:1.3.0",
      artifact_sha256 = "97dc45afefe3a1e421da42b8b6e9f90491477c45fc6178203e3a5e8a05ee8553",
  )
