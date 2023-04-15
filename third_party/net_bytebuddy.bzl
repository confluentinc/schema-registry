load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "net_bytebuddy_byte_buddy",
      artifact = "net.bytebuddy:byte-buddy:1.12.10",
      artifact_sha256 = "1a1ac9ce65eddcea54ead958387bb0b3863d02a2ffe856ab6a57ac79737c19cf",
  )


  import_external(
      name = "net_bytebuddy_byte_buddy_agent",
      artifact = "net.bytebuddy:byte-buddy-agent:1.12.10",
      artifact_sha256 = "5e8606d14a844c1ec70d2eb8f50c4009fb16138905dee8ca50a328116c041257",
  )
