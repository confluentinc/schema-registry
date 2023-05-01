load("@rules_java//java:defs.bzl", "java_binary", "java_library")
load("@aspect_bazel_lib//lib:write_source_files.bzl", "write_source_files")

def generate_openapi_spec(
        properties_file,
        output_file,
        additional_runtime_deps = []):
    java_binary(
        name = "_bin_generate_openapi_spec",
        main_class = "io.confluent.bazel.swagger.BazelSwaggerCoreMain",
        resources = [properties_file],
        runtime_deps = [
            "//swagger-core-bazel:bazel_swagger_core_lib",
        ] + additional_runtime_deps,
    )

    native.genrule(
        name = "_run_generate_openapi_spec",
        srcs = [":_bin_generate_openapi_spec_deploy.jar"],
        outs = [output_file],
        cmd = "java -jar $(location :_bin_generate_openapi_spec_deploy.jar) $@",
    )

    # copy the spec to the source tree

    write_source_files(
        name = "_copy_spec_to_src",
        files = {
            output_file: output_file,
        },
        diff_test = False,
    )
