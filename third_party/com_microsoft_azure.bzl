load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_microsoft_azure_msal4j",
      artifact = "com.microsoft.azure:msal4j:1.13.3",
      artifact_sha256 = "a1bf775b679f2113b520f59166833867638e5d085565e0feeb8ae3adc14983cf",
      deps = [
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_nimbusds_oauth2_oidc_sdk",
          "@net_minidev_json_smart",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "com_microsoft_azure_msal4j_persistence_extension",
      artifact = "com.microsoft.azure:msal4j-persistence-extension:1.1.0",
      artifact_sha256 = "fedf58b0f8e55a9ff06c8e197cf9c9c74417b894960316526c5452728f3d46d3",
      deps = [
          "@com_microsoft_azure_msal4j",
          "@net_java_dev_jna_jna",
          "@net_java_dev_jna_jna_platform",
          "@org_slf4j_slf4j_api"
      ],
  )
