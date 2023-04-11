load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "javax_annotation_javax_annotation_api",
      artifact = "javax.annotation:javax.annotation-api:1.3.2",
      artifact_sha256 = "e04ba5195bcd555dc95650f7cc614d151e4bcd52d29a10b8aa2197f3ab89ab9b",
  )


  import_external(
      name = "javax_annotation_jsr250_api",
      artifact = "javax.annotation:jsr250-api:1.0",
      artifact_sha256 = "a1a922d0d9b6d183ed3800dfac01d1e1eb159f0e8c6f94736931c1def54a941f",
  )
