load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "javax_xml_bind_jaxb_api",
      artifact = "javax.xml.bind:jaxb-api:2.3.0",
      artifact_sha256 = "883007989d373d19f352ba9792b25dec21dc7d0e205a710a93a3815101bb3d03",
  )
