load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_glassfish_hk2_external_aopalliance_repackaged",
      artifact = "org.glassfish.hk2.external:aopalliance-repackaged:2.6.1",
      artifact_sha256 = "bad77f9278d753406360af9e4747bd9b3161554ea9cd3d62411a0ae1f2c141fd",
  )


  import_external(
      name = "org_glassfish_hk2_external_jakarta_inject",
      artifact = "org.glassfish.hk2.external:jakarta.inject:2.6.1",
      artifact_sha256 = "5e88c123b3e41bca788b2683118867d9b6dec714247ea91c588aed46a36ee24f",
    # EXCLUDES javax.inject:javax.inject
  )
