load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "javax_xml_soap_javax_xml_soap_api",
      artifact = "javax.xml.soap:javax.xml.soap-api:1.4.0",
      artifact_sha256 = "141374e33be99768611a2d42b9d33571a0c5b9763beca9c2dc90900d8cc8f767",
  )
