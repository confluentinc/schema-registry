load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "org_apache_directory_server_apacheds_core",
      artifact = "org.apache.directory.server:apacheds-core:2.0.0-M22",
      artifact_sha256 = "7196b88991fcff619fa7a432bb177386c3b4f925399017e7a2639badd212a8d1",
      deps = [
          "@commons_lang_commons_lang",
          "@org_apache_directory_api_api_ldap_codec_core",
          "@org_apache_directory_api_api_ldap_extras_util",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_apache_directory_server_apacheds_interceptors_admin",
          "@org_apache_directory_server_apacheds_interceptors_authn",
          "@org_apache_directory_server_apacheds_interceptors_authz",
          "@org_apache_directory_server_apacheds_interceptors_changelog",
          "@org_apache_directory_server_apacheds_interceptors_collective",
          "@org_apache_directory_server_apacheds_interceptors_event",
          "@org_apache_directory_server_apacheds_interceptors_exception",
          "@org_apache_directory_server_apacheds_interceptors_journal",
          "@org_apache_directory_server_apacheds_interceptors_normalization",
          "@org_apache_directory_server_apacheds_interceptors_number",
          "@org_apache_directory_server_apacheds_interceptors_operational",
          "@org_apache_directory_server_apacheds_interceptors_referral",
          "@org_apache_directory_server_apacheds_interceptors_schema",
          "@org_apache_directory_server_apacheds_interceptors_subtree",
          "@org_apache_directory_server_apacheds_interceptors_trigger",
          "@org_bouncycastle_bcprov_jdk15on",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_core_api",
      artifact = "org.apache.directory.server:apacheds-core-api:2.0.0-M22",
      artifact_sha256 = "6f18103017ca1d12d36e726ea7ac7c20ccef84024395480528a819ecb98df161",
      deps = [
          "@commons_lang_commons_lang",
          "@net_sf_ehcache_ehcache",
          "@org_apache_directory_api_api_asn1_api",
          "@org_apache_directory_api_api_i18n",
          "@org_apache_directory_api_api_ldap_client_api",
          "@org_apache_directory_api_api_ldap_codec_core",
          "@org_apache_directory_api_api_ldap_extras_aci",
          "@org_apache_directory_api_api_ldap_extras_util",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_constants",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_apache_mina_mina_core",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.apache.directory.api:api-ldap-schema-data
  )


  import_external(
      name = "org_apache_directory_server_apacheds_core_avl",
      artifact = "org.apache.directory.server:apacheds-core-avl:2.0.0-M22",
      artifact_sha256 = "6f94d1487c00b24b9a0f28da273566f46bda6cb42c9f087938ae07827bf41c51",
      deps = [
          "@commons_lang_commons_lang",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_core_constants",
      artifact = "org.apache.directory.server:apacheds-core-constants:2.0.0-M22",
      artifact_sha256 = "85a958d53390b796158d88ae00d4244aeb691f533d76380fd88581be0a33a462",
      deps = [
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_core_shared",
      artifact = "org.apache.directory.server:apacheds-core-shared:2.0.0-M22",
      artifact_sha256 = "9959d6b8b47840de101d171143c8c4e1a7f6fcedd56bd4af3d6ebd8999536178",
      deps = [
          "@net_sf_ehcache_ehcache",
          "@org_apache_directory_api_api_ldap_codec_core",
          "@org_apache_directory_api_api_ldap_extras_codec_api",
          "@org_apache_directory_api_api_ldap_extras_util",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_jdbm_apacheds_jdbm1",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_i18n",
      artifact = "org.apache.directory.server:apacheds-i18n:2.0.0-M22",
      artifact_sha256 = "b30f49d7ee90dff2ce1c055a82c4af840dbcffe5f3b4a124c4e1ef0a32c52355",
      deps = [
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptor_kerberos",
      artifact = "org.apache.directory.server:apacheds-interceptor-kerberos:2.0.0-M22",
      artifact_sha256 = "848f449fa4ad8b0654002244fea66b10857bb2b1bff38aa02fb51e0c2dd713cb",
      deps = [
          "@org_apache_directory_api_api_asn1_api",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_apache_directory_server_apacheds_kerberos_codec",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.apache.directory.api:api-ldap-schema-data
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_admin",
      artifact = "org.apache.directory.server:apacheds-interceptors-admin:2.0.0-M22",
      artifact_sha256 = "d599863f037c33c1a88ae3c8f4846078532d8f8cbb475b4e155b70af3a473122",
      deps = [
          "@org_apache_directory_api_api_ldap_extras_util",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_authn",
      artifact = "org.apache.directory.server:apacheds-interceptors-authn:2.0.0-M22",
      artifact_sha256 = "fa0d2d66d767f551083be09491446b9d271252d60b5f707ffec548683ca09cbd",
      deps = [
          "@commons_collections_commons_collections",
          "@commons_lang_commons_lang",
          "@org_apache_directory_api_api_ldap_client_api",
          "@org_apache_directory_api_api_ldap_extras_codec",
          "@org_apache_directory_api_api_ldap_extras_codec_api",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_apache_mina_mina_core",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_authz",
      artifact = "org.apache.directory.server:apacheds-interceptors-authz:2.0.0-M22",
      artifact_sha256 = "a40df8916201e8bbfb861d13ac0946bc292303b319f69c4a825edb249833918f",
      deps = [
          "@net_sf_ehcache_ehcache",
          "@org_apache_directory_api_api_ldap_extras_aci",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_changelog",
      artifact = "org.apache.directory.server:apacheds-interceptors-changelog:2.0.0-M22",
      artifact_sha256 = "f140decc14259dfef4640e7c5479edb574da5cdf4de1384c1386e89651957686",
      deps = [
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_collective",
      artifact = "org.apache.directory.server:apacheds-interceptors-collective:2.0.0-M22",
      artifact_sha256 = "77b20b83414c938b3e5654795c87e88221d05d65951006aeb72841ffb10190b9",
      deps = [
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_event",
      artifact = "org.apache.directory.server:apacheds-interceptors-event:2.0.0-M22",
      artifact_sha256 = "3122a0a708ca35a2526632857a28f22d118722b94aa83dc00d6756986b0c4010",
      deps = [
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_exception",
      artifact = "org.apache.directory.server:apacheds-interceptors-exception:2.0.0-M22",
      artifact_sha256 = "9b51f89a1191c7a4c833ad63e4b90f2b2449642162b9a05874ba86c1139d4a58",
      deps = [
          "@commons_collections_commons_collections",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_journal",
      artifact = "org.apache.directory.server:apacheds-interceptors-journal:2.0.0-M22",
      artifact_sha256 = "b96770fbc7054dcd53838338910ddc8ba08502e160118c24b7e00159637b18b2",
      deps = [
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_normalization",
      artifact = "org.apache.directory.server:apacheds-interceptors-normalization:2.0.0-M22",
      artifact_sha256 = "aa6a161351aaa106ac303adc1f381ec589b2c2be054883431160d50ef9b143ab",
      deps = [
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_number",
      artifact = "org.apache.directory.server:apacheds-interceptors-number:2.0.0-M22",
      artifact_sha256 = "553141d7d995356ed0e2a3691258883ae85824df4af85ccfd49f7de6d02e0e9f",
      deps = [
          "@org_apache_directory_api_api_ldap_extras_util",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_operational",
      artifact = "org.apache.directory.server:apacheds-interceptors-operational:2.0.0-M22",
      artifact_sha256 = "e343616dc80aa8f507a520e56ef5350da7bf306eed8438796731797e9c88f7fe",
      deps = [
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_referral",
      artifact = "org.apache.directory.server:apacheds-interceptors-referral:2.0.0-M22",
      artifact_sha256 = "d119a728dddb9d84a9454b34c1644a965e32045a7b22e1684bff6696e2c613fd",
      deps = [
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_schema",
      artifact = "org.apache.directory.server:apacheds-interceptors-schema:2.0.0-M22",
      artifact_sha256 = "3f7d359e0afcd63f322a6a00919fbb2ee19cee024122dcc470ee60333ccdcc04",
      deps = [
          "@org_apache_directory_api_api_i18n",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_subtree",
      artifact = "org.apache.directory.server:apacheds-interceptors-subtree:2.0.0-M22",
      artifact_sha256 = "591aefb32c5832334ec9ad907399abc7812b3214c9662472c40959af6a6d78bf",
      deps = [
          "@net_sf_ehcache_ehcache",
          "@org_apache_directory_api_api_ldap_codec_core",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_interceptors_trigger",
      artifact = "org.apache.directory.server:apacheds-interceptors-trigger:2.0.0-M22",
      artifact_sha256 = "a0b55d845901425158a7d4b73314f032d979a109fda6e49b42ad085490b92a3e",
      deps = [
          "@org_apache_directory_api_api_ldap_extras_trigger",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_jdbm_partition",
      artifact = "org.apache.directory.server:apacheds-jdbm-partition:2.0.0-M22",
      artifact_sha256 = "8b0291b10fd046af4e31ed19a703f9595b61f0c38f114e0006b3fafbba8f4eaf",
      deps = [
          "@commons_lang_commons_lang",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_jdbm_apacheds_jdbm1",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_avl",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_apache_directory_server_apacheds_xdbm_partition",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.apache.directory.api:api-ldap-schema-data
  )


  import_external(
      name = "org_apache_directory_server_apacheds_kerberos_codec",
      artifact = "org.apache.directory.server:apacheds-kerberos-codec:2.0.0-M22",
      artifact_sha256 = "a91c9b46793b1f51cb4f24526abcf599beb678894432473cddbce053857070f9",
      deps = [
          "@net_sf_ehcache_ehcache",
          "@org_apache_directory_api_api_asn1_api",
          "@org_apache_directory_api_api_asn1_ber",
          "@org_apache_directory_api_api_i18n",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )


  import_external(
      name = "org_apache_directory_server_apacheds_ldif_partition",
      artifact = "org.apache.directory.server:apacheds-ldif-partition:2.0.0-M22",
      artifact_sha256 = "203d850f05c46823eec4693c18dbdf57ea61d558385a8158824a98733dd3525d",
      deps = [
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_apache_directory_server_apacheds_xdbm_partition",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.apache.directory.api:api-ldap-schema-data
  )


  import_external(
      name = "org_apache_directory_server_apacheds_mavibot_partition",
      artifact = "org.apache.directory.server:apacheds-mavibot-partition:2.0.0-M22",
      artifact_sha256 = "9d80cbcb14b401aa1e6c4424083e45fdd1833c812eb57341e872c86becd7753b",
      deps = [
          "@commons_lang_commons_lang",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_mavibot_mavibot",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_avl",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_apache_directory_server_apacheds_xdbm_partition",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.apache.directory.api:api-ldap-schema-data
  )


  import_external(
      name = "org_apache_directory_server_apacheds_protocol_kerberos",
      artifact = "org.apache.directory.server:apacheds-protocol-kerberos:2.0.0-M22",
      artifact_sha256 = "44506ac8a3e9551b7170640fe8a244f18e4b72cbd40d97b4fc8466ff253f0c35",
      deps = [
          "@net_sf_ehcache_ehcache",
          "@org_apache_directory_api_api_asn1_api",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_apache_directory_server_apacheds_kerberos_codec",
          "@org_apache_directory_server_apacheds_protocol_shared",
          "@org_apache_mina_mina_core",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.apache.directory.api:api-ldap-schema-data
  )


  import_external(
      name = "org_apache_directory_server_apacheds_protocol_ldap",
      artifact = "org.apache.directory.server:apacheds-protocol-ldap:2.0.0-M22",
      artifact_sha256 = "f71f67ba59cf9744d3ee42441988e77c0b8c25c5a3e7d576b00c4527228608c9",
      deps = [
          "@commons_lang_commons_lang",
          "@org_apache_directory_api_api_asn1_ber",
          "@org_apache_directory_api_api_ldap_client_api",
          "@org_apache_directory_api_api_ldap_codec_core",
          "@org_apache_directory_api_api_ldap_extras_codec",
          "@org_apache_directory_api_api_ldap_extras_codec_api",
          "@org_apache_directory_api_api_ldap_extras_sp",
          "@org_apache_directory_api_api_ldap_extras_util",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_jdbm_apacheds_jdbm1",
          "@org_apache_directory_server_apacheds_core",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_apache_directory_server_apacheds_jdbm_partition",
          "@org_apache_directory_server_apacheds_kerberos_codec",
          "@org_apache_directory_server_apacheds_protocol_shared",
          "@org_apache_mina_mina_core",
          "@org_bouncycastle_bcprov_jdk15on",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.apache.directory.api:api-ldap-schema-data
  )


  import_external(
      name = "org_apache_directory_server_apacheds_protocol_shared",
      artifact = "org.apache.directory.server:apacheds-protocol-shared:2.0.0-M22",
      artifact_sha256 = "e46cf98f5eadda48c5671d6f6b203a0a05b7a7bda0273057d25c2f47c236c010",
      deps = [
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_apache_directory_server_apacheds_kerberos_codec",
          "@org_apache_mina_mina_core",
          "@org_slf4j_slf4j_api"
      ],
    # EXCLUDES org.apache.directory.api:api-ldap-schema-data
  )


  import_external(
      name = "org_apache_directory_server_apacheds_xdbm_partition",
      artifact = "org.apache.directory.server:apacheds-xdbm-partition:2.0.0-M22",
      artifact_sha256 = "851e9990da68cc9f3ad434175238949dfab0ef9ae0afdbdfecb2cd2e13fafeef",
      deps = [
          "@commons_collections_commons_collections",
          "@org_apache_directory_api_api_ldap_model",
          "@org_apache_directory_api_api_util",
          "@org_apache_directory_server_apacheds_core_api",
          "@org_apache_directory_server_apacheds_core_avl",
          "@org_apache_directory_server_apacheds_core_shared",
          "@org_apache_directory_server_apacheds_i18n",
          "@org_slf4j_slf4j_api"
      ],
  )
