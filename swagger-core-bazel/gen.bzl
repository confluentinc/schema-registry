load("@rules_java//java:defs.bzl", "java_binary", "java_library")
load("@aspect_bazel_lib//lib:write_source_files.bzl", "write_source_files")

def generate_openapi_spec(
        properties_file,
        config_file,
        output_files,
        additional_runtime_deps = []):
    java_binary(
        name = "_bin_generate_openapi_spec",
        main_class = "io.confluent.bazel.swagger.SwaggerCoreBazel",
        resources = [properties_file, config_file],
        runtime_deps = [
            "//swagger-core-bazel:swagger_core_bazel_lib",
        ] + additional_runtime_deps,
    )

    native.genrule(
        name = "_run_generate_openapi_spec",
        srcs = [":_bin_generate_openapi_spec_deploy.jar"],
        outs = output_files,
        cmd = "java -jar $(location :_bin_generate_openapi_spec_deploy.jar) $(OUTS)",
    )

    # covert output_files to a dict frm a list
    output_files = dict(zip(output_files, output_files))

    # copy the spec(s) to the source tree
    write_source_files(
        name = "_copy_spec_to_src",
        files = output_files,
        diff_test = False,
    )
