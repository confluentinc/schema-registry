load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_google_auto_service_auto_service_annotations",
      artifact = "com.google.auto.service:auto-service-annotations:1.0.1",
      artifact_sha256 = "c7bec54b7b5588b5967e870341091c5691181d954cf2039f1bf0a6eeb837473b",
  )
