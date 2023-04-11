load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "commons_codec_commons_codec",
      artifact = "commons-codec:commons-codec:1.10",
      artifact_sha256 = "4241dfa94e711d435f29a4604a3e2de5c4aa3c165e23bd066be6fc1fc4309569",
  )
