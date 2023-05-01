load("@rules_java//java:defs.bzl", "java_binary", "java_library")

def generate_openapi_spec(
        properties_file,
        additional_runtime_deps = []):
    java_binary(
        name = "generate_openapi_spec",
        main_class = "io.confluent.bazel.swagger.BazelSwaggerCoreMain",
        resources = [
            #            "src/main/resources/swagger.properties",
            properties_file,
        ],
        runtime_deps = [
            "//swagger-core-bazel:bazel_swagger_core_lib",
        ] + additional_runtime_deps,
    )
