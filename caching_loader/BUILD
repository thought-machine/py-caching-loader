package(
    default_python_interpreter = 'python3.5',
)

python_test(
    name = 'caching_loader_test',
    srcs = ['caching_loader_test.py'],
    deps = [
        ':caching_loader',
        '//third_party:promise',
    ],
)

python_library(
    name = 'caching_loader',
    srcs = ['caching_loader.py'],
    deps = [
        '//third_party:promise',
    ],
)
