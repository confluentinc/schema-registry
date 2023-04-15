load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_jboss_logging_jboss_logging",
      artifact = "org.jboss.logging:jboss-logging:3.3.2.Final",
      artifact_sha256 = "cb914bfe888da7d9162e965ac8b0d6f28f2f32eca944a00fbbf6dd3cf1aacc13",
  )
