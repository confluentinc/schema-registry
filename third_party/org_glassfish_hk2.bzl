load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_glassfish_hk2_hk2_api",
      artifact = "org.glassfish.hk2:hk2-api:2.6.1",
      artifact_sha256 = "c2cb80a01e58440ae57d5ee59af4d4d94e5180e04aff112b0cb611c07d61e773",
      deps = [
          "@org_glassfish_hk2_external_aopalliance_repackaged",
          "@org_glassfish_hk2_external_jakarta_inject",
          "@org_glassfish_hk2_hk2_utils"
      ],
  )


  import_external(
      name = "org_glassfish_hk2_hk2_locator",
      artifact = "org.glassfish.hk2:hk2-locator:2.6.1",
      artifact_sha256 = "febc668deb9f2000c76bd4918d8086c0a4c74d07bd0c60486b72c6bd38b62874",
      deps = [
          "@org_glassfish_hk2_external_aopalliance_repackaged",
          "@org_glassfish_hk2_external_jakarta_inject",
          "@org_glassfish_hk2_hk2_api",
          "@org_glassfish_hk2_hk2_utils"
      ],
    # EXCLUDES jakarta.annotation:jakarta.annotation-api
    # EXCLUDES org.javassist:javassist
  )


  import_external(
      name = "org_glassfish_hk2_hk2_utils",
      artifact = "org.glassfish.hk2:hk2-utils:2.6.1",
      artifact_sha256 = "30727f79086452fdefdab08451d982c2082aa239d9f75cdeb1ba271e3c887036",
      deps = [
          "@org_glassfish_hk2_external_jakarta_inject"
      ],
  )


  import_external(
      name = "org_glassfish_hk2_osgi_resource_locator",
      artifact = "org.glassfish.hk2:osgi-resource-locator:1.0.3",
      artifact_sha256 = "aab5d7849f7cfcda2cc7c541ba1bd365151d42276f151c825387245dfde3dd74",
  )
