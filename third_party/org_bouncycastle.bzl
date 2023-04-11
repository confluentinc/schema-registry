load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_bouncycastle_bcpkix_jdk15on",
      artifact = "org.bouncycastle:bcpkix-jdk15on:1.68",
      artifact_sha256 = "fb8d0f8f673ad6e16c604732093d7aa31b26ff4e0bd9cae1d7f99984c06b8a0f",
      deps = [
          "@org_bouncycastle_bcprov_jdk15on"
      ],
  )


  import_external(
      name = "org_bouncycastle_bcprov_jdk15on",
      artifact = "org.bouncycastle:bcprov-jdk15on:1.53",
      artifact_sha256 = "84c45d3d5552ab29381a55a419fbd093e1bd63032fb803f34816ae69fbbef2e5",
  )
