load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_servicemix_bundles_org_apache_servicemix_bundles_antlr",
      artifact = "org.apache.servicemix.bundles:org.apache.servicemix.bundles.antlr:2.7.7_5",
      artifact_sha256 = "3902794d36d9b81da1b7e697f21ed04ccae276cc116eecc640a4cd0fff2691f2",
  )


  import_external(
      name = "org_apache_servicemix_bundles_org_apache_servicemix_bundles_dom4j",
      artifact = "org.apache.servicemix.bundles:org.apache.servicemix.bundles.dom4j:1.6.1_5",
      artifact_sha256 = "15abe1ccad24f4fd71a926959f1acd64d84878348deee12dcf4928ee4f1db3d5",
  )


  import_external(
      name = "org_apache_servicemix_bundles_org_apache_servicemix_bundles_xpp3",
      artifact = "org.apache.servicemix.bundles:org.apache.servicemix.bundles.xpp3:1.1.4c_6",
      artifact_sha256 = "81da51b0b2b919f9070c3cce7ba26c8516c65cc15737f3659469fd3fa06c3e53",
  )
