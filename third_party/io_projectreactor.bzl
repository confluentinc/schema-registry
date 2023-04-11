load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "io_projectreactor_reactor_core",
      artifact = "io.projectreactor:reactor-core:3.4.23",
      artifact_sha256 = "d03b2f2eb5da79d0283358cba6ff36456982800f6cd2666857af46a43b6a9eb2",
      deps = [
          "@org_reactivestreams_reactive_streams"
      ],
  )
