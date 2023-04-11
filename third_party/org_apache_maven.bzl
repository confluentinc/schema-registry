load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_maven_maven_artifact",
      artifact = "org.apache.maven:maven-artifact:3.0",
      artifact_sha256 = "759079b9cf0cddae5ba06c96fd72347d82d0bc1d903c95d398c96522b139e470",
      deps = [
          "@org_codehaus_plexus_plexus_utils"
      ],
  )


  import_external(
      name = "org_apache_maven_maven_model",
      artifact = "org.apache.maven:maven-model:3.8.1",
      artifact_sha256 = "9e008629cefa5ddc9e5e2628adb467416ced250591e30dea7103f37f513b3b13",
      deps = [
          "@org_codehaus_plexus_plexus_utils"
      ],
  )


  import_external(
      name = "org_apache_maven_maven_plugin_api",
      artifact = "org.apache.maven:maven-plugin-api:3.8.1",
      artifact_sha256 = "9a2d71722c8c18db748195925e725359e4e87d6722b602a0e54095e91a7f30d3",
      deps = [
          "@org_apache_maven_maven_artifact",
          "@org_apache_maven_maven_model",
          "@org_codehaus_plexus_plexus_classworlds",
          "@org_codehaus_plexus_plexus_utils",
          "@org_eclipse_sisu_org_eclipse_sisu_plexus"
      ],
  )
