#load("@rules_jvm_external//:defs.bzl", "maven_install")
#
#def swagger_deps():
#    maven_install(
#        artifacts = [
#            "io.swagger.core.v3:swagger-core:2.2.9",
#            "io.swagger.core.v3:swagger-integration:2.2.9",
#            "io.swagger.core.v3:swagger-jaxrs2:2.2.9",
#            "org.codehaus.plexus:plexus-utils:3.5.1",
#
#            # reflectively loaded by swagger-core
#            "javax.ws.rs:javax.ws.rs-api:2.1.1",
#            "javax.servlet:javax.servlet-api:4.0.1",
#        ],
#        # maven_install_json = "//:maven_install.json",
#        repositories = [
#            # Private repositories are supported through HTTP Basic auth
#            "https://confluent-519856050701.d.codeartifact.us-west-2.amazonaws.com/maven/maven/",
#            "https://confluent-519856050701.dp.confluent.io/maven/maven-public/",
#            "https://packages.confluent.io/maven/",
#            "https://jitpack.io",
#            "https://oss.sonatype.org/content/repositories/snapshots",
#            "https://repo1.maven.org/maven2",
#            "https://repository.apache.org/snapshots",
#            "https://maven.google.com",
#        ],
#        use_credentials_from_home_netrc_file = True,
#    )
