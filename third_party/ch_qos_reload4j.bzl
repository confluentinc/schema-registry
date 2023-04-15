load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "ch_qos_reload4j_reload4j",
      artifact = "ch.qos.reload4j:reload4j:1.2.19",
      artifact_sha256 = "fa07aa7adedf2a65eb09007443bb60b51c725b1b5b88e481e00c529ff2d3b5b5",
  )
