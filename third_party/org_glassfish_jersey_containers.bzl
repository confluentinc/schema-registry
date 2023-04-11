load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_glassfish_jersey_containers_jersey_container_servlet",
      artifact = "org.glassfish.jersey.containers:jersey-container-servlet:2.34",
      deps = [
          "@jakarta_ws_rs_jakarta_ws_rs_api",
          "@org_glassfish_jersey_containers_jersey_container_servlet_core",
          "@org_glassfish_jersey_core_jersey_common",
          "@org_glassfish_jersey_core_jersey_server"
      ],
  )


  import_external(
      name = "org_glassfish_jersey_containers_jersey_container_servlet_core",
      artifact = "org.glassfish.jersey.containers:jersey-container-servlet-core:2.34",
      deps = [
          "@jakarta_ws_rs_jakarta_ws_rs_api",
          "@org_glassfish_hk2_external_jakarta_inject",
          "@org_glassfish_jersey_core_jersey_common",
          "@org_glassfish_jersey_core_jersey_server"
      ],
    # EXCLUDES javax.servlet:servlet-api
    # EXCLUDES jakarta.servlet:jakarta.servlet-api
  )
