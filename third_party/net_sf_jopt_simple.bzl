load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "net_sf_jopt_simple_jopt_simple",
      artifact = "net.sf.jopt-simple:jopt-simple:5.0.4",
      artifact_sha256 = "df26cc58f235f477db07f753ba5a3ab243ebe5789d9f89ecf68dd62ea9a66c28",
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )
