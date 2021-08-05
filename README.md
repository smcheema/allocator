Allocator
---

### Contributing

```sh
bazel build ...
bazel test ... --test_output=all \
  --cache_test_results=no \
  --test_arg='-test.v' \
  --test_filter='Test.*'

bazel run //:gazelle
bazel run //:gazelle -- update-repos -from_file=go.mod -prune=true
```
