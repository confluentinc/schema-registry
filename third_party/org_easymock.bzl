load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_easymock_easymock",
      artifact = "org.easymock:easymock:4.3",
      artifact_sha256 = "c230864c8b11636aaa6bb49eee00a4342d3e016d860b4f80b89068fd056d1404",
      deps = [
          "@org_objenesis_objenesis"
      ],
  )
