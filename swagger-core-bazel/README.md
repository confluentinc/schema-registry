# Swagger Core Bazel

This is a Bazel target for generating a swagger spec. It is a wrapper
around the [swagger-core](todo) project and
is an adaptation of the [maven plugin](todo) within that project.

The `generate_openapi_spec` rule will output a yaml and/or json openapi spec.
Input properties include:
* `additional_runtime_deps` - additional dependencies to include in the runtime classpath.
You must supply the rest resources that swagger is expected to scan.
* `config_file` - the path to the [openapi config](todo) yaml file.
* `output_files` - the output files to generate. Should be yaml and/or json. The filetype is inferred
from the extension.
* `properties_file` - the path to the properties file. The properties file really just tells the code where to find the
openapi config file. But it can also override other settings.

```BUILD
generate_openapi_spec(
    additional_runtime_deps = [
        "//core:core",
    ],
    config_file = "src/main/resources/openapi.yaml",
    output_files = [
        "output/schema-registry-api-spec.yaml",
#        "output/schema-registry-api-spec.json"
        ],
    properties_file = "src/main/resources/swagger-core-bazel.properties",
)

```