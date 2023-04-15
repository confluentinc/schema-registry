load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_eclipse_sisu_org_eclipse_sisu_inject",
      artifact = "org.eclipse.sisu:org.eclipse.sisu.inject:0.3.4",
      artifact_sha256 = "8c0e6aa7f35593016f2c5e78b604b57f023cdaca3561fe2fe36f2b5dbbae1d16",
  )


  import_external(
      name = "org_eclipse_sisu_org_eclipse_sisu_plexus",
      artifact = "org.eclipse.sisu:org.eclipse.sisu.plexus:0.3.4",
      artifact_sha256 = "87e66ffad03aa18129ea0762d2c02f566a9480e6eee8d84e25e1b931f12ea831",
      deps = [
          "@javax_enterprise_cdi_api",
          "@org_codehaus_plexus_plexus_classworlds",
          "@org_codehaus_plexus_plexus_component_annotations",
          "@org_codehaus_plexus_plexus_utils",
          "@org_eclipse_sisu_org_eclipse_sisu_inject"
      ],
  )
