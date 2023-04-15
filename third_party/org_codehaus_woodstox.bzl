load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_codehaus_woodstox_stax2_api",
      artifact = "org.codehaus.woodstox:stax2-api:4.2.1",
      artifact_sha256 = "678567e48b51a42c65c699f266539ad3d676d4b1a5b0ad7d89ece8b9d5772579",
    # EXCLUDES javax.xml.stream:stax-api
  )
