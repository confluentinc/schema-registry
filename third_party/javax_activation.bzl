load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "javax_activation_activation",
      artifact = "javax.activation:activation:1.1.1",
      artifact_sha256 = "ae475120e9fcd99b4b00b38329bd61cdc5eb754eee03fe66c01f50e137724f99",
  )
