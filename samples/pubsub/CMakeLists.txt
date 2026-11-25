# Copyright (c) 2026 Cliff Brake
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.20.0)

# Build against the module this sample lives in, so no manifest entry is needed.
list(APPEND ZEPHYR_EXTRA_MODULES ${CMAKE_CURRENT_SOURCE_DIR}/../..)

find_package(Zephyr REQUIRED HINTS $ENV{ZEPHYR_BASE})
project(nats_pubsub_sample)

target_sources(app PRIVATE src/main.c)
