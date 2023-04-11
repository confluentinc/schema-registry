load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_mockito_mockito_core",
      artifact = "org.mockito:mockito-core:4.6.1",
      artifact_sha256 = "ee3b91cdf4c23cff92960c32364371c683ee6415f1ec4678317bcea79c9f9819",
      deps = [
          "@net_bytebuddy_byte_buddy",
          "@net_bytebuddy_byte_buddy_agent"
      ],
      runtime_deps = [
          "@org_objenesis_objenesis"
      ],
  )


  import_external(
      name = "org_mockito_mockito_inline",
      artifact = "org.mockito:mockito-inline:4.6.1",
      artifact_sha256 = "ee52e1c299a632184fba274a9370993e09140429f5e516e6c5570fd6574b297f",
      deps = [
          "@org_mockito_mockito_core"
      ],
  )
