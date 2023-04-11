load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_reactivestreams_reactive_streams",
      artifact = "org.reactivestreams:reactive-streams:1.0.4",
      artifact_sha256 = "f75ca597789b3dac58f61857b9ac2e1034a68fa672db35055a8fb4509e325f28",
  )
