load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "com_amazonaws_aws_java_sdk_core",
      artifact = "com.amazonaws:aws-java-sdk-core:1.12.182",
      artifact_sha256 = "c747b045ebab86fca389148b04d33fa726f67c328ba7522c30ef2a4186f704e2",
      deps = [
          "@com_fasterxml_jackson_core_jackson_databind",
          "@com_fasterxml_jackson_dataformat_jackson_dataformat_cbor",
          "@commons_codec_commons_codec",
          "@commons_logging_commons_logging",
          "@joda_time_joda_time",
          "@org_apache_httpcomponents_httpclient",
          "@software_amazon_ion_ion_java"
      ],
  )


  import_external(
      name = "com_amazonaws_aws_java_sdk_kms",
      artifact = "com.amazonaws:aws-java-sdk-kms:1.12.182",
      artifact_sha256 = "2750944e6cf33b8672bc26f3488b9b19f605292dad37cf646b5383f9e647566e",
      deps = [
          "@com_amazonaws_aws_java_sdk_core",
          "@com_amazonaws_jmespath_java"
      ],
  )


  import_external(
      name = "com_amazonaws_jmespath_java",
      artifact = "com.amazonaws:jmespath-java:1.12.182",
      artifact_sha256 = "79ca0c0b977053debc975bbd37166ed5d64fdfaf0fbcab1a2940d72b68708f2c",
      deps = [
          "@com_fasterxml_jackson_core_jackson_databind"
      ],
  )
