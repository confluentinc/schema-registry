load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_directory_api_api_all",
      artifact = "org.apache.directory.api:api-all:1.0.0-M33",
      artifact_sha256 = "0960f6b3919a46956b7f602bc82649922ff67ec9555814a97729a0309af2730a",
      deps = [
          "@commons_collections_commons_collections",
          "@commons_io_commons_io",
          "@commons_lang_commons_lang",
          "@commons_pool_commons_pool",
          "@org_apache_mina_mina_core",
          "@org_apache_servicemix_bundles_org_apache_servicemix_bundles_antlr",
          "@org_apache_servicemix_bundles_org_apache_servicemix_bundles_dom4j",
          "@org_apache_servicemix_bundles_org_apache_servicemix_bundles_xpp3",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES xml-apis:xml-apis
    # EXCLUDES org.apache.directory.api:api-ldap-schema-data
  )


  import_external(
      name = "org_apache_directory_api_api_asn1_api",
      artifact = "org.apache.directory.api:api-asn1-api:1.0.0-RC1",
      artifact_sha256 = "c2cb9b4d9975e5f51381c77f2b287ea429236d0bc4ed3f3ff08ee30fea4010c0",
      deps = [
          "@org_apache_directory_api_api_i18n"
      ],
  )


  import_external(
      name = "org_apache_directory_api_api_asn1_ber",
      artifact = "org.apache.directory.api:api-asn1-ber:1.0.0-RC1",
      artifact_sha256 = "53ca9598466a9fe9a3ede87cc0742dc2c8a497fe629fc71511430d2ebb973bd2",
      deps = [
          "@org_apache_directory_api_api_asn1_api",
          "@org_apache_directory_api_api_i18n",
          "@org_apache_directory_api_api_util",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_api_api_i18n",
      artifact = "org.apache.directory.api:api-i18n:1.0.0-RC1",
      artifact_sha256 = "1b928b5b35d8fd3ac84c071ee49f866febe6c7fcffb4a4d4af4854110b9a159f",
  )


  import_external(
      name = "org_apache_directory_api_api_ldap_client_api",
      artifact = "org.apache.directory.api:api-ldap-client-api:1.0.0-RC1",
      artifact_sha256 = "675f8641357b5844526e5fc5fd1819a5b0ecf9d5157bc0892a994693c810a0df",
      deps = [
          "@commons_pool_commons_pool",
          "@org_apache_directory_api_api_ldap_codec_core",
          "@org_apache_directory_api_api_ldap_extras_aci",
          "@org_apache_directory_api_api_ldap_extras_codec",
          "@org_apache_directory_api_api_ldap_extras_codec_api",
          "@org_apache_mina_mina_core"
      ],
  )


  import_external(
      name = "org_apache_directory_api_api_ldap_codec_core",
      artifact = "org.apache.directory.api:api-ldap-codec-core:1.0.0-RC1",
      artifact_sha256 = "06b5160fce9ea9594df25ff840457d5292d4947f1d6859b87b35e62e0144de15",
      deps = [
          "@commons_collections_commons_collections",
          "@commons_lang_commons_lang",
          "@org_apache_directory_api_api_asn1_api",
          "@org_apache_directory_api_api_asn1_ber",
          "@org_apache_directory_api_api_i18n",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_mina_mina_core"
      ],
  )


  import_external(
      name = "org_apache_directory_api_api_ldap_extras_aci",
      artifact = "org.apache.directory.api:api-ldap-extras-aci:1.0.0-RC1",
      artifact_sha256 = "11fdacd39c2b1ac151a94161d4f97f4255dcac85c78d24f8fed3b10e8a6ed13e",
      deps = [
          "@org_apache_directory_api_api_i18n",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_servicemix_bundles_org_apache_servicemix_bundles_antlr"
      ],
  )


  import_external(
      name = "org_apache_directory_api_api_ldap_extras_codec",
      artifact = "org.apache.directory.api:api-ldap-extras-codec:1.0.0-RC1",
      artifact_sha256 = "4b979d7a37793a009251461a29b09ffa896e6474bfb452d40cbb7b03278cf0c6",
      deps = [
          "@org_apache_directory_api_api_ldap_codec_core",
          "@org_apache_directory_api_api_ldap_extras_codec_api"
      ],
  )


  import_external(
      name = "org_apache_directory_api_api_ldap_extras_codec_api",
      artifact = "org.apache.directory.api:api-ldap-extras-codec-api:1.0.0-RC1",
      artifact_sha256 = "a1b6e1533378bad664c911ee2f620ff4dd891eb60945e0909f9b5ed084337d87",
      deps = [
          "@org_apache_directory_api_api_ldap_model"
      ],
  )


  import_external(
      name = "org_apache_directory_api_api_ldap_extras_sp",
      artifact = "org.apache.directory.api:api-ldap-extras-sp:1.0.0-RC1",
      artifact_sha256 = "4e8e25a7c04a42d48ca43aaa7b8c8b7c098425d345f71b970e19627f2fb38f1c",
      deps = [
          "@commons_lang_commons_lang",
          "@org_apache_directory_api_api_i18n",
          "@org_apache_directory_api_api_ldap_extras_codec",
          "@org_apache_directory_api_api_ldap_extras_util",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util"
      ],
  )


  import_external(
      name = "org_apache_directory_api_api_ldap_extras_trigger",
      artifact = "org.apache.directory.api:api-ldap-extras-trigger:1.0.0-RC1",
      artifact_sha256 = "44af0f8ad28adb455ce76f9612e3fbbc84b376a0a36c353a0dd335d81771288b",
      deps = [
          "@commons_lang_commons_lang",
          "@org_apache_directory_api_api_i18n",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_servicemix_bundles_org_apache_servicemix_bundles_antlr"
      ],
  )


  import_external(
      name = "org_apache_directory_api_api_ldap_extras_util",
      artifact = "org.apache.directory.api:api-ldap-extras-util:1.0.0-RC1",
      artifact_sha256 = "2670704d330f70c6ead4ad0e7d0395ff2e9e96a96ef42fb66cb3862e3b14fd32",
      deps = [
          "@org_apache_directory_api_api_i18n",
          "@org_apache_directory_api_api_ldap_codec_core",
          "@org_apache_directory_api_api_ldap_model"
      ],
  )


  import_external(
      name = "org_apache_directory_api_api_ldap_model",
      artifact = "org.apache.directory.api:api-ldap-model:1.0.0-RC1",
      artifact_sha256 = "2f441865cc7766e156c59be6cd7a6153b52c18e537de59025c22e8d585abc8be",
      deps = [
          "@commons_codec_commons_codec",
          "@commons_collections_commons_collections",
          "@commons_lang_commons_lang",
          "@org_apache_directory_api_api_asn1_api",
          "@org_apache_directory_api_api_asn1_ber",
          "@org_apache_directory_api_api_i18n",
          "@org_apache_directory_api_api_util",
          "@org_apache_mina_mina_core",
          "@org_apache_servicemix_bundles_org_apache_servicemix_bundles_antlr"
      ],
  )


  import_external(
      name = "org_apache_directory_api_api_util",
      artifact = "org.apache.directory.api:api-util:1.0.0-RC1",
      artifact_sha256 = "8196b4a71d01c04c321d38da32649add8cd8485d2a52ffc32f8b130b9ecfee51",
      deps = [
          "@org_apache_directory_api_api_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )
