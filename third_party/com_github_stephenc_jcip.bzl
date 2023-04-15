load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_github_stephenc_jcip_jcip_annotations",
      artifact = "com.github.stephenc.jcip:jcip-annotations:1.0-1",
      artifact_sha256 = "4fccff8382aafc589962c4edb262f6aa595e34f1e11e61057d1c6a96e8fc7323",
  )
