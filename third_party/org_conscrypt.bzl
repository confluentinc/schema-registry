load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_conscrypt_conscrypt_openjdk_uber",
      artifact = "org.conscrypt:conscrypt-openjdk-uber:2.5.2",
      artifact_sha256 = "eaf537d98e033d0f0451cd1b8cc74e02d7b55ec882da63c88060d806ba89c348",
  )
