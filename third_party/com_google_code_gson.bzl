load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_code_gson_gson",
      artifact = "com.google.code.gson:gson:2.8.6",
  )
