load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_glassfish_jersey_core_jersey_client",
      artifact = "org.glassfish.jersey.core:jersey-client:2.36",
      artifact_sha256 = "027b7061001f186bd7a48bbe5f070e2774b108969776935fc9b55591a93f689d",
      deps = [
          "@jakarta_ws_rs_jakarta_ws_rs_api",
          "@org_glassfish_hk2_external_jakarta_inject",
          "@org_glassfish_jersey_core_jersey_common"
      ],
  )


  import_external(
      name = "org_glassfish_jersey_core_jersey_common",
      artifact = "org.glassfish.jersey.core:jersey-common:2.36",
      artifact_sha256 = "543b8df0bfa07e54fe65e45b351088010a2a7079ac2564023761f8dfe8eb7b33",
      deps = [
          "@jakarta_annotation_jakarta_annotation_api",
          "@jakarta_ws_rs_jakarta_ws_rs_api",
          "@org_glassfish_hk2_external_jakarta_inject",
          "@org_glassfish_hk2_osgi_resource_locator"
      ],
  )


  import_external(
      name = "org_glassfish_jersey_core_jersey_server",
      artifact = "org.glassfish.jersey.core:jersey-server:2.36",
      artifact_sha256 = "2699758d1c33a9137363fd022d8c9c00423c800c4fde2b49d53530987e8da72d",
      deps = [
          "@jakarta_annotation_jakarta_annotation_api",
          "@jakarta_validation_jakarta_validation_api",
          "@jakarta_ws_rs_jakarta_ws_rs_api",
          "@org_glassfish_hk2_external_jakarta_inject",
          "@org_glassfish_jersey_core_jersey_client",
          "@org_glassfish_jersey_core_jersey_common"
      ],
  )
