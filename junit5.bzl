load("//:import_external.bzl", import_external = "safe_exodus_maven_import_external")
"""External dependencies & java_junit5_test rule"""

JUNIT_JUPITER_GROUP_ID = "org.junit.jupiter"
JUNIT_JUPITER_ARTIFACT_ID_LIST = [
    "junit-jupiter-api",
    "junit-jupiter-engine",
    "junit-jupiter-params",
]

JUNIT_PLATFORM_GROUP_ID = "org.junit.platform"
JUNIT_PLATFORM_ARTIFACT_ID_LIST = [
    "junit-platform-commons",
    "junit-platform-console",
    "junit-platform-engine",
    "junit-platform-launcher",
    "junit-platform-suite-api",
]

JUNIT_EXTRA_DEPENDENCIES = [
    ("org.apiguardian", "apiguardian-api", "1.0.0"),
    ("org.opentest4j", "opentest4j", "1.1.1"),
]

def junit_jupiter_java_repositories(
        version = "5.4.2"):
    """Imports dependencies for JUnit Jupiter"""
    for artifact_id in JUNIT_JUPITER_ARTIFACT_ID_LIST:
        import_external(
            name = _format_maven_jar_name(JUNIT_JUPITER_GROUP_ID, artifact_id),
            artifact = "%s:%s:%s" % (
                JUNIT_JUPITER_GROUP_ID,
                artifact_id,
                version,
            ),
        )

    for t in JUNIT_EXTRA_DEPENDENCIES:
        import_external(
            name = _format_maven_jar_name(t[0], t[1]),
            artifact = "%s:%s:%s" % t,
        )

def junit_platform_java_repositories(
        version = "1.4.2"):
    """Imports dependencies for JUnit Platform"""
    for artifact_id in JUNIT_PLATFORM_ARTIFACT_ID_LIST:
        import_external(
            name = _format_maven_jar_name(JUNIT_PLATFORM_GROUP_ID, artifact_id),
            artifact = "%s:%s:%s" % (
                JUNIT_PLATFORM_GROUP_ID,
                artifact_id,
                version,
            ),
        )

def java_junit5_test(name, srcs, test_package = None, deps = [], runtime_deps = [], **kwargs):
    FILTER_KWARGS = [
        "main_class",
        "use_testrunner",
        "args",
    ]

    for arg in FILTER_KWARGS:
        if arg in kwargs.keys():
            kwargs.pop(arg)

    junit_console_args = []
    if test_package:
        junit_console_args += ["--select-package", test_package]
    else:
        fail("must specify 'test_package'")

    native.java_test(
        name = name,
        srcs = srcs,
        use_testrunner = False,
        main_class = "org.junit.platform.console.ConsoleLauncher",
        args = junit_console_args,
        deps = deps + [
            _format_maven_jar_dep_name(JUNIT_JUPITER_GROUP_ID, artifact_id)
            for artifact_id in JUNIT_JUPITER_ARTIFACT_ID_LIST
        ] + [
            _format_maven_jar_dep_name(JUNIT_PLATFORM_GROUP_ID, "junit-platform-suite-api"),
        ] + [
            _format_maven_jar_dep_name(t[0], t[1])
            for t in JUNIT_EXTRA_DEPENDENCIES
        ],
        runtime_deps = runtime_deps + [
            _format_maven_jar_dep_name(JUNIT_PLATFORM_GROUP_ID, artifact_id)
            for artifact_id in JUNIT_PLATFORM_ARTIFACT_ID_LIST
        ],
        **kwargs
    )

def _format_maven_jar_name(group_id, artifact_id):
    return ("exodus_%s_%s" % (group_id, artifact_id)).replace(".", "_").replace("-", "_")

def _format_maven_jar_dep_name(group_id, artifact_id):
    return "@%s//jar" % _format_maven_jar_name(group_id, artifact_id)