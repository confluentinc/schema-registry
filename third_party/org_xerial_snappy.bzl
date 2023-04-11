load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_xerial_snappy_snappy_java",
      artifact = "org.xerial.snappy:snappy-java:1.1.9.1",
      artifact_sha256 = "b696bc9105d1e869de3e5b2bb165bd0a482bf08415917e25d29c7461e5b1496e",
  )
