load("@io_bazel_rules_scala//scala:scala.bzl", "scala_specs2_junit_test","scala_library")
load("@io_bazel_rules_scala//specs2:specs2_junit.bzl", "specs2_junit_dependencies")

target_test_classes = "target/test-classes"

_unit_prefixes = ["Test"]
_unit_suffixes = _unit_prefixes
_unit_tags = ["UT"]

_it_prefixes = ["IT", "E2E"]
_it_suffixes = _it_prefixes
_it_tags = ["IT", "E2E"]

_mixed_prefixes = _unit_prefixes + _it_prefixes
_mixed_suffixes = _mixed_prefixes
_mixed_tags = _unit_tags + _it_tags

def specs2_unit_test(extra_runtime_dirs = [target_test_classes],
                     extra_runtime_entries = [target_test_classes],
                     **kwargs):
  size = kwargs.pop("size", "small")
  timeout = kwargs.pop("timeout", None)

  _add_test_target(
      _unit_prefixes,
      _unit_suffixes,
      _unit_tags,
      False,
      size,
      timeout,
      extra_runtime_dirs,
      extra_runtime_entries,
      **kwargs
  )

def specs2_ite2e_test(block_network = True,
                      extra_runtime_dirs = [target_test_classes],
                      extra_runtime_entries = [target_test_classes],
                      **kwargs):
  timeout = kwargs.pop("timeout", _default_moderate_timeout_or_implied_from_size_attr(kwargs))
  size = kwargs.pop("size", "large")

  _add_test_target(
      _it_prefixes,
      _it_suffixes,
      _it_tags,
      block_network,
      size,
      timeout,
      extra_runtime_dirs,
      extra_runtime_entries,
      **kwargs
  )

def specs2_mixed_test(block_network = True,
                      extra_runtime_dirs = [target_test_classes],
                      extra_runtime_entries = [target_test_classes],
                      **kwargs):
  timeout = kwargs.pop("timeout", _default_moderate_timeout_or_implied_from_size_attr(kwargs))
  size = kwargs.pop("size", "large")

  _add_test_target(
      _mixed_prefixes,
      _mixed_suffixes,
      _mixed_tags,
      block_network,
      size,
      timeout,
      extra_runtime_dirs,
      extra_runtime_entries,
      **kwargs
  )

def _add_test_target(prefixes,
                     suffixes,
                     test_tags,
                     block_network,
                     size,
                     timeout,
                     extra_runtime_dirs,
                     extra_runtime_entries,
                     **kwargs):
  #extract attribute(s) common to both test and scala_library
  name = kwargs.pop("name")
  user_test_tags = kwargs.pop("tags", test_tags)
  #Bazel idiomatic wise `data` is needed in both.
  #(scala_library for other tests that might need the data in the runfiles and the test needs it so that it can do $location expansion)
  data = kwargs.pop("data", [])
  #extract attributes which are only for the test runtime
  end_prefixes = kwargs.pop("prefixes", prefixes)
  end_suffixes = kwargs.pop("suffixes", suffixes)
  jvm_flags = kwargs.pop("jvm_flags", [])
  flaky = kwargs.pop("flaky", None)
  shard_count = kwargs.pop("shard_count", None)
  args = kwargs.pop("args", None)
  local = kwargs.pop("local", None)
  deps = kwargs.pop("deps",[])

  jvm_flags.extend([
    #   "-javaagent:$(rootpath @core_server_build_tools//test-agent/src/main/java/com/wixpress/agent:test-agent_deploy.jar)",
      "-Dextra.dirs=" + ":".join(extra_runtime_dirs),
  ])

#   data.append("@core_server_build_tools//test-agent/src/main/java/com/wixpress/agent:test-agent_deploy.jar")

  # test-agent-deploy jar is added to bootcalsspath becasue when the test classpath is very long
  # and the test is executed using a manifest jar for some unknown reason the JVM fails to find
  # the agent jar
#   runtime_entries = [
#       "$(rootpath @core_server_build_tools//test-agent/src/main/java/com/wixpress/agent:test-agent_deploy.jar)"
#   ]

#   if extra_runtime_entries != None:
#       runtime_entries.extend(extra_runtime_entries)

#   jvm_flags.append("-Xbootclasspath/a:" + ":".join(runtime_entries))

  #mitigate issue where someone explicitly adds testonly in their kwargs and so we get it twice
  testonly = kwargs.pop("testonly", 1)

  junit_specs2_deps = specs2_junit_dependencies() + [
    "//external:io_bazel_rules_scala/dependency/junit/junit",
    "//external:io_bazel_rules_scala/dependency/hamcrest/hamcrest_core",
  ]

  scala_library(
      name = name,
      tags = user_test_tags,
      data = data,
      testonly = testonly,
      deps = junit_specs2_deps + deps,
      **kwargs
  )

  scala_specs2_junit_test(
      name = name + "_test_runner",
      prefixes = end_prefixes,
      suffixes = end_suffixes,
      deps = deps,
      runtime_deps = [":" + name],
      tests_from = [":" + name],
      jvm_flags = jvm_flags,
      size = size,
      timeout = timeout,
      flaky = flaky,
      shard_count = shard_count,
      args = args,
      local = local,
      data = data,
      tags = _test_tags(user_test_tags, block_network),
    #   exec_compatible_with = _constraint_by_size(size)
  )

# def _constraint_by_size(size):
#   large_machine_constraint = ["@core_server_build_tools//platforms:large_machine"]

#   if size in ["large", "enormous"]:
#     return large_machine_constraint
#   else:
#     return None

def _test_tags(test_tags, block_network):
  tags = []
  if (block_network):
    tags = ["block-network"]

  return tags + test_tags

def _default_moderate_timeout_or_implied_from_size_attr(kwargs):
  if 'size' in kwargs:
    #let bazel imply timeout from the size
    default_timeout = None
  else:
    default_timeout = "moderate"
