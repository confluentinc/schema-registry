load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_fasterxml_classmate",
      artifact = "com.fasterxml:classmate:1.3.4",
      artifact_sha256 = "c2bfcc21467351d0f9a1558822b72dbac2b21f6b9f700a44fc6b345491ef3c88",
  )
