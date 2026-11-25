# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project uses
[semantic versioning](https://semver.org/).

## [unreleased]

- **NATS client for Zephyr** — devices publish, subscribe, join queue groups,
  and make requests with headers on any NATS server.
- **Stays connected on its own** — after a dropped or stalled connection the
  client reconnects with backoff and restores its subscriptions.
- **Predictable memory use** — the client allocates nothing at run time; buffer
  sizes and limits are set in Kconfig.
- **Secured servers** — token, user and password, and TLS. TLS needs the
  server's `handshake_first` option.
