load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_fasterxml_jackson_core_jackson_annotations",
      artifact = "com.fasterxml.jackson.core:jackson-annotations:2.13.4",
      artifact_sha256 = "ac5b27a634942391ca113850ee7db01df1499a240174021263501c05fc653b44",
  )


  import_external(
      name = "com_fasterxml_jackson_core_jackson_core",
      artifact = "com.fasterxml.jackson.core:jackson-core:2.14.2",
      artifact_sha256 = "b5d37a77c88277b97e3593c8740925216c06df8e4172bbde058528df04ad3e7a",
  )


  import_external(
      name = "com_fasterxml_jackson_core_jackson_databind",
      artifact = "com.fasterxml.jackson.core:jackson-databind:2.14.2",
      artifact_sha256 = "501d3abce4d18dcc381058ec593c5b94477906bba6efbac14dae40a642f77424",
      deps = [
          "@com_fasterxml_jackson_core_jackson_annotations",
          "@com_fasterxml_jackson_core_jackson_core"
      ],
  )
