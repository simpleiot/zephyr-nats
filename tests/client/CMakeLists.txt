# Copyright (c) 2026 Cliff Brake
# SPDX-License-Identifier: Apache-2.0

cmake_minimum_required(VERSION 3.20.0)

list(APPEND ZEPHYR_EXTRA_MODULES ${CMAKE_CURRENT_SOURCE_DIR}/../..)

find_package(Zephyr REQUIRED HINTS $ENV{ZEPHYR_BASE})
project(nats_client_test)

# The tests use the internal URL parser.
target_include_directories(app PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../src)
target_sources(app PRIVATE src/main.c)

# The TLS test needs the CA that signed the test server's certificate.
if(CONFIG_NATS_TLS AND DEFINED NATS_TEST_CA)
  generate_inc_file_for_target(app ${NATS_TEST_CA}
    ${ZEPHYR_BINARY_DIR}/include/generated/nats_test_ca.inc)
  target_compile_definitions(app PRIVATE NATS_TEST_HAVE_CA)
endif()
