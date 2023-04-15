load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")

def dependencies():

  import_external(
      name = "javax_websocket_javax_websocket_api",
      artifact = "javax.websocket:javax.websocket-api:1.0",
      artifact_sha256 = "dd93009fb5aa3798bcd9ab0492a292ddae0f0b1ed2e45a75867a9925c90e747a",
  )


  import_external(
      name = "javax_websocket_javax_websocket_client_api",
      artifact = "javax.websocket:javax.websocket-client-api:1.0",
      artifact_sha256 = "0102ee41121ed7b8834f1efc6ef1a0dbaf610c1ad8f0d941ad721662b17bd27e",
  )
