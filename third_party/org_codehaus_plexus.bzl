load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_codehaus_plexus_plexus_classworlds",
      artifact = "org.codehaus.plexus:plexus-classworlds:2.6.0",
      artifact_sha256 = "52f77c5ec49f787c9c417ebed5d6efd9922f44a202f217376e4f94c0d74f3549",
  )


  import_external(
      name = "org_codehaus_plexus_plexus_component_annotations",
      artifact = "org.codehaus.plexus:plexus-component-annotations:1.5.5",
      artifact_sha256 = "4df7a6a7be64b35bbccf60b5c115697f9ea3421d22674ae67135dde375fcca1f",
  )


  import_external(
      name = "org_codehaus_plexus_plexus_utils",
      artifact = "org.codehaus.plexus:plexus-utils:3.2.1",
      artifact_sha256 = "8d07b497bb8deb167ee5329cae87ef2043833bf52e4f15a5a9379cec447a5b2b",
  )
