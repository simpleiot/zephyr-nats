# Copyright (c) 2026 Cliff Brake
# SPDX-License-Identifier: Apache-2.0

if(CONFIG_NATS)

# The client uses Zephyr's namespaced networking API (zsock_*, NET_AF_INET,
# struct net_sockaddr_in), which arrived in 4.4. zephyr/module.yml has no
# field for a kernel version, so check it here.
set(NATS_MIN_ZEPHYR_VERSION 4.4)

if("${KERNEL_VERSION_MAJOR}.${KERNEL_VERSION_MINOR}.${KERNEL_PATCHLEVEL}"
   VERSION_LESS ${NATS_MIN_ZEPHYR_VERSION})
  message(FATAL_ERROR
    "zephyr-nats requires Zephyr ${NATS_MIN_ZEPHYR_VERSION} or later, but "
    "this workspace is ${KERNEL_VERSION_STRING}.")
endif()

zephyr_library_named(nats)

zephyr_include_directories(include)

zephyr_library_sources(
  src/nats_client.c
  src/nats_parser.c
  src/nats_util.c
)

endif()
