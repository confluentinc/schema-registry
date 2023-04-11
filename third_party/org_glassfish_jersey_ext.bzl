load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_glassfish_jersey_ext_jersey_bean_validation",
      artifact = "org.glassfish.jersey.ext:jersey-bean-validation:2.36",
      artifact_sha256 = "9be74faf1a2a3011d326bb38300c86e6cec194d83515874127c871940d592879",
      deps = [
          "@jakarta_el_jakarta_el_api",
          "@jakarta_validation_jakarta_validation_api",
          "@jakarta_ws_rs_jakarta_ws_rs_api",
          "@org_glassfish_hk2_external_jakarta_inject",
          "@org_glassfish_jakarta_el",
          "@org_glassfish_jersey_core_jersey_common",
          "@org_glassfish_jersey_core_jersey_server"
      ],
    # EXCLUDES org.hibernate.validator:hibernate-validator
  )
