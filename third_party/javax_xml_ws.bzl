load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "javax_xml_ws_jaxws_api",
      artifact = "javax.xml.ws:jaxws-api:2.3.0",
      artifact_sha256 = "c261f75c1a25ecb17d1936efe34a34236b5d0e79415b34ffb9324359a30a8c08",
      deps = [
          "@javax_xml_bind_jaxb_api",
          "@javax_xml_soap_javax_xml_soap_api"
      ],
  )
