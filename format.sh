#!/usr/bin/env bash

clang-format -i include/pubsub/*.hpp
clang-format -i examples/*.cpp
clang-format -i perf/*.cpp
clang-format -i perf/*.hpp
clang-format -i tests/*.cpp
clang-format -i tests/*.hpp
clang-format -i src/pubsub/*.cpp
clang-format -i src/pubsub/*.hpp