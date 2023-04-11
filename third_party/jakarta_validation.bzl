load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "jakarta_validation_jakarta_validation_api",
      artifact = "jakarta.validation:jakarta.validation-api:2.0.2",
      artifact_sha256 = "b42d42428f3d922c892a909fa043287d577c0c5b165ad9b7d568cebf87fc9ea4",
  )
