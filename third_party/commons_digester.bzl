load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "commons_digester_commons_digester",
      artifact = "commons-digester:commons-digester:2.1",
      artifact_sha256 = "e0b2b980a84fc6533c5ce291f1917b32c507f62bcad64198fff44368c2196a3d",
    # EXCLUDES commons-beanutils:commons-beanutils
    # EXCLUDES commons-logging:commons-logging
  )
