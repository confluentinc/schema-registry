load("@rules_java//java:defs.bzl", "java_binary", "java_library")
load("@aspect_bazel_lib//lib:write_source_files.bzl", "write_source_files")

def generate_openapi_spec(
        properties_file,
        config_file,
        output_files,
        additional_runtime_deps = []):
    java_library(
        name = "swagger_core_bazel_lib",
        srcs = [
            "//swagger-core-bazel:src/main/java/io/confluent/bazel/swagger/SwaggerCoreBazel.java",
        ],
        deps = [
            "@swagger_deps//:com_fasterxml_jackson_core_jackson_core",
            "@swagger_deps//:io_swagger_core_v3_swagger_core",
            "@swagger_deps//:io_swagger_core_v3_swagger_integration",
            "@swagger_deps//:io_swagger_core_v3_swagger_jaxrs2",
            "@swagger_deps//:io_swagger_core_v3_swagger_models",
            "@swagger_deps//:javax_servlet_javax_servlet_api",
            "@swagger_deps//:javax_ws_rs_javax_ws_rs_api",
            "@swagger_deps//:org_codehaus_plexus_plexus_utils",
            "@swagger_deps//:org_slf4j_slf4j_api",
        ],
    )

    java_binary(
        name = "_bin_generate_openapi_spec",
        main_class = "io.confluent.bazel.swagger.SwaggerCoreBazel",
        resources = [properties_file, config_file],
        runtime_deps = [
            ":swagger_core_bazel_lib",
        ] + additional_runtime_deps,
    )

    # add a leading underscore to get around https://github.com/aspect-build/bazel-lib/issues/291
    modified_output_files = []
    for output_file in output_files:
        modified_output_files.append("_" + output_file)

    native.genrule(
        name = "_run_generate_openapi_spec",
        srcs = [":_bin_generate_openapi_spec_deploy.jar"],
        outs = modified_output_files,
        cmd = "java -jar $(location :_bin_generate_openapi_spec_deploy.jar) $(OUTS)",
    )

    # covert output_files to a dict frm a list
    output_files_dict = dict(zip(output_files, modified_output_files))

    # copy the spec(s) to the source tree
    write_source_files(
        name = "_copy_spec_to_src",
        files = output_files_dict,
        diff_test = True,
    )
