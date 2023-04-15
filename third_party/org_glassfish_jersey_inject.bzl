load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_glassfish_jersey_inject_jersey_hk2",
      artifact = "org.glassfish.jersey.inject:jersey-hk2:2.34",
      deps = [
          "@org_glassfish_hk2_hk2_locator",
          "@org_glassfish_jersey_core_jersey_common",
          "@org_javassist_javassist"
      ],
  )
