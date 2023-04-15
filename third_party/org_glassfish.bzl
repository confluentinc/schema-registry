load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_glassfish_jakarta_el",
      artifact = "org.glassfish:jakarta.el:3.0.4",
      artifact_sha256 = "3b8d4311b47fb47d168ad4338b6649a7cc21d5066b9765bd28ebca93148064be",
  )
