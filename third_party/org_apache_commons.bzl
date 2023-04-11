load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_commons_commons_compress",
      artifact = "org.apache.commons:commons-compress:1.21",
      artifact_sha256 = "6aecfd5459728a595601cfa07258d131972ffc39b492eb48bdd596577a2f244a",
  )


  import_external(
      name = "org_apache_commons_commons_lang3",
      artifact = "org.apache.commons:commons-lang3:3.12.0",
      artifact_sha256 = "d919d904486c037f8d193412da0c92e22a9fa24230b9d67a57855c5c31c7e94e",
  )


  import_external(
      name = "org_apache_commons_commons_text",
      artifact = "org.apache.commons:commons-text:1.10.0",
      artifact_sha256 = "770cd903fa7b604d1f7ef7ba17f84108667294b2b478be8ed1af3bffb4ae0018",
      deps = [
          "@org_apache_commons_commons_lang3"
      ],
  )
