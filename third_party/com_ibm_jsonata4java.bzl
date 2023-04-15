load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_ibm_jsonata4java_JSONata4Java",
      artifact = "com.ibm.jsonata4java:JSONata4Java:2.2.5",
      artifact_sha256 = "9cd95e532ca4085e86a45890c3b8e63a9c25335c9dca5f91270e24ce960e9cb8",
      deps = [
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_jackson_dataformat_jackson_dataformat_xml",
          "@com_fasterxml_woodstox_woodstox_core",
          "@com_google_code_gson_gson",
          "@org_antlr_antlr4_runtime",
          "@org_apache_commons_commons_text"
      ],
  )
