#!/usr/bin/env bash

clang-format -i include/pubsub/*.hpp
clang-format -i examples/*.cpp
clang-format -i src/pubsub/*.cpp
clang-format -i src/pubsub/*.hpp