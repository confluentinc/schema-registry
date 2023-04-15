load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_agrona_agrona",
      artifact = "org.agrona:agrona:1.17.1",
      artifact_sha256 = "ca6ff763fd93ddcf5dd2b8b9f60dc977a2d644f1f1666ffd9cf7fd637ab6d3fd",
  )
