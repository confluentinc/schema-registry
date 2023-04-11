load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "net_sourceforge_argparse4j_argparse4j",
      artifact = "net.sourceforge.argparse4j:argparse4j:0.7.0",
      artifact_sha256 = "65ceb669d88f63306c680f8088bbf765bbca72d288a61a03703000076c3d3f56",
    # EXCLUDES *:javax
    # EXCLUDES *:mail
    # EXCLUDES *:jmxri
    # EXCLUDES *:jms
    # EXCLUDES *:jmxtools
    # EXCLUDES *:jline
  )
