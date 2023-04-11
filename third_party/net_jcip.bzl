load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "net_jcip_jcip_annotations",
      artifact = "net.jcip:jcip-annotations:1.0",
      artifact_sha256 = "be5805392060c71474bf6c9a67a099471274d30b83eef84bfc4e0889a4f1dcc0",
  )
