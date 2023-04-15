load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "jakarta_xml_bind_jakarta_xml_bind_api",
      artifact = "jakarta.xml.bind:jakarta.xml.bind-api:2.3.2",
      artifact_sha256 = "69156304079bdeed9fc0ae3b39389f19b3cc4ba4443bc80508995394ead742ea",
      deps = [
          "@jakarta_activation_jakarta_activation_api"
      ],
  )
