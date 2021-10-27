load("@bazel_tools//tools/build_defs/repo:http.bzl", "http_archive")
load("@bazel_tools//tools/build_defs/repo:git.bzl", "git_repository")

git_repository(
    name = "ortools",
    commit = "b37d9c786b69128f3505f15beca09e89bf078a89",  #tag v9.0
    remote = "https://github.com/google/or-tools.git",
)

git_repository(
    name = "com_github_gflags_gflags",
    commit = "e171aa2",  # release v2.2.2
    remote = "https://github.com/gflags/gflags.git",
)

git_repository(
    name = "com_github_glog_glog",
    commit = "96a2f23",  # release v0.4.0
    remote = "https://github.com/google/glog.git",
)

git_repository(
    name = "com_google_protobuf",
    commit = "436bd78",  # release v3.15.8
    remote = "https://github.com/protocolbuffers/protobuf.git",
)

git_repository(
    name = "com_google_absl",
    commit = "e1d388e",  # release 20210324.1
    remote = "https://github.com/abseil/abseil-cpp.git",
)

load("@com_google_protobuf//:protobuf_deps.bzl", "protobuf_deps")

# Load common dependencies.
protobuf_deps()

http_archive(
    name = "io_bazel_rules_go",
    sha256 = "8e968b5fcea1d2d64071872b12737bbb5514524ee5f0a4f54f5920266c261acb",
    urls = [
        "https://mirror.bazel.build/github.com/bazelbuild/rules_go/releases/download/v0.28.0/rules_go-v0.28.0.zip",
        "https://github.com/bazelbuild/rules_go/releases/download/v0.28.0/rules_go-v0.28.0.zip",
    ],
)

git_repository(
    name = "com_github_irfansharif_solver",
    branch = "6b95fa8eade20aa6ccc93d2748f41891a73a92b6",
    remote = "https://github.com/irfansharif/solver",

    # To point to local checkout, use:
    #
    #   remote = "/Users/irfansharif/Software/src/github.com/irfansharif/solver",
)

git_repository(
    name = "bazel_gazelle",
    commit = "d038863ba2e096792c6bb6afca31f6514f1aeecd",
    remote = "https://github.com/bazelbuild/bazel-gazelle",
)

# Load up our go dependencies (the ones listed under go.mod). The `DEPS.bzl`
# file is kept up to date using the `update-repos` Gazelle command (see
# README).
#
# gazelle:repository_macro DEPS.bzl%go_deps
load("//:DEPS.bzl", "go_deps")

# VERY IMPORTANT that we call into this function to prefer our pinned versions
# of the dependencies to any that might be pulled in via functions like
# `go_rules_dependencies`, `gazelle_dependencies`, etc.
go_deps()

load("@io_bazel_rules_go//go:deps.bzl", "go_register_toolchains", "go_rules_dependencies")

go_rules_dependencies()

go_register_toolchains(go_version = "1.16.5")

# Load gazelle dependencies.
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")

gazelle_dependencies()
