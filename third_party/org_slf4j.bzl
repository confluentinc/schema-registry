load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_slf4j_slf4j_api",
      artifact = "org.slf4j:slf4j-api:1.7.36",
      artifact_sha256 = "d3ef575e3e4979678dc01bf1dcce51021493b4d11fb7f1be8ad982877c16a1c0",
  )


  import_external(
      name = "org_slf4j_slf4j_log4j12",
      artifact = "org.slf4j:slf4j-log4j12:1.7.25",
    # EXCLUDES *:*
  )


  import_external(
      name = "org_slf4j_slf4j_reload4j",
      artifact = "org.slf4j:slf4j-reload4j:1.7.36",
      artifact_sha256 = "ae6fdd5c9547896114d5ec7fa7503733b7d2890573d3886fb548b3119c4d3f67",
      deps = [
          "@ch_qos_reload4j_reload4j",
          "@org_slf4j_slf4j_api"
      ],
  )
