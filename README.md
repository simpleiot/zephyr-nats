# zephyr-nats

A [NATS](https://nats.io) client for [Zephyr RTOS](https://zephyrproject.org),
packaged as a Zephyr module.

It lets an MCU with a TCP connection publish, subscribe, and make requests on a
NATS server, using the same subjects as the rest of the system. It is written
for constrained devices: no allocation by the client, fixed buffers sized in
Kconfig, and a single receive thread.

## Status

The core protocol is implemented and tested on `native_sim` against
`nats-server`. It has not yet been measured on hardware. Requires Zephyr 4.4 or
later.

## Features

Core protocol (first release):

- `INFO`, `CONNECT`, `PUB`, `HPUB`, `SUB`, `UNSUB`, `MSG`, `HMSG`, `PING`,
  `PONG`, `+OK`, `-ERR`
- Message headers (NATS/1.0)
- Request/reply with a per-connection inbox
- Queue groups
- Server and client PINGs, with a configurable stale-connection limit
- Automatic reconnect with backoff, resubscribing on reconnect
- TLS through Zephyr's TLS sockets (mbed TLS), with server verification. The
  client starts TLS before the server sends `INFO`, so the server needs
  `handshake_first: true` (see [TLS](#tls)).
- Authentication: token, user and password

Later:

- NKey and JWT (`.creds`) authentication, which need Ed25519 signatures
- JetStream: publish with acknowledgment, and pull consumers (`Fetch`)
- Multiple servers in a cluster, from the `connect_urls` in `INFO`

Not planned:

- Push consumers, the key-value and object store APIs, and WebSocket transport

## Design

- **No allocation.** The application provides the client and subscription
  structs, usually as statics. Receive and transmit buffers are fixed, sized by
  `CONFIG_NATS_RX_BUF_SIZE` and `CONFIG_NATS_TX_BUF_SIZE`. A message larger than
  the receive buffer is dropped and counted, and the connection stays up.
  Numeric server addresses connect directly. A hostname needs
  `CONFIG_DNS_RESOLVER` and is resolved with `getaddrinfo()`, which uses the
  system heap (`CONFIG_HEAP_MEM_POOL_SIZE`).
- **One thread per client.** A receive thread reads the socket, parses
  protocol lines, answers PINGs, and calls subscription callbacks. Callbacks
  run in that thread, so they should return quickly or hand work to another
  thread (a work queue or zbus channel).
- **Thread-safe publish.** `nats_publish()` and `nats_request()` can be called
  from any thread. Writes are serialized with a mutex. Messages are not
  buffered while disconnected; publishing returns `-ENOTCONN`.
- **Always reconnecting.** After `nats_connect()`, the client retries with
  jittered exponential backoff until `nats_disconnect()`, and restores its
  subscriptions on each new connection.
- **Incremental parser.** The protocol parser is a state machine that does not
  depend on sockets, so it can be tested on its own and fed partial reads.
- **Zephyr networking.** The client uses the socket API (`zsock_*`), so it
  works over Ethernet, Wi-Fi, cellular, and native_sim.

## API

```c
#include <zephyr/net/nats.h>

static struct nats_client client;
static struct nats_sub cmd_sub;

static void on_cmd(struct nats_client *c, const struct nats_msg *msg,
                   void *user_data)
{
	/* Subjects and payloads are NUL-terminated. */
	printk("%s: %s\n", msg->subject, (const char *)msg->data);

	if (msg->reply_len > 0) {
		nats_respond(c, msg, "ok", 2);
	}
}

int main(void)
{
	const struct nats_config cfg = {
		.url = "nats://demo.nats.io:4222",
		.name = "sensor-1",
	};

	nats_client_init(&client, &cfg);
	nats_subscribe(&client, &cmd_sub, "dev.sensor-1.cmd", NULL, on_cmd, NULL);
	nats_connect(&client, K_SECONDS(10));

	nats_publish(&client, "dev.sensor-1.temp", "21.5", 4);

	return 0;
}
```

Main functions:

| Function               | Purpose                                                 |
| ---------------------- | ------------------------------------------------------- |
| `nats_client_init()`   | Set up a client from a config                           |
| `nats_connect()`       | Start the receive thread and wait for the connection    |
| `nats_disconnect()`    | Flush, close, and stop reconnecting                     |
| `nats_is_connected()`  | Check whether the connection is ready                   |
| `nats_publish()`       | Publish a message                                       |
| `nats_publish_reply()` | Publish a message with a reply subject                  |
| `nats_publish_hdr()`   | Publish a message with headers                          |
| `nats_subscribe()`     | Subscribe, with optional queue group                    |
| `nats_unsubscribe()`   | Unsubscribe now, or after N messages                    |
| `nats_request()`       | Publish and wait for one reply, with a timeout          |
| `nats_respond()`       | Reply to a received message                             |
| `nats_flush()`         | Send a PING and wait for the PONG                       |
| `nats_msg_header()`    | Read a header from a received message                   |
| `nats_client_stats()`  | Read message, byte, drop, and reconnect counters        |

Connection events (connected, disconnected, reconnected, server error) are
reported through `event_cb`. `nats_request()` returns `-EHOSTUNREACH` when no
subscriber is listening, using the server's no-responders status. Functions
that wait (`nats_request()`, `nats_flush()`, `nats_disconnect()`) return
`-EDEADLK` when called from a callback. See
[`include/zephyr/net/nats.h`](include/zephyr/net/nats.h) for details.

## Configuration

| Kconfig option                     | Default | Description                                  |
| ---------------------------------- | ------- | -------------------------------------------- |
| `CONFIG_NATS`                      | n       | Enable the NATS client                       |
| `CONFIG_NATS_RX_BUF_SIZE`          | 1024    | Largest protocol line plus payload received  |
| `CONFIG_NATS_TX_BUF_SIZE`          | 512     | Buffer for outgoing protocol lines           |
| `CONFIG_NATS_MAX_SUBS`             | 8       | Subscriptions per client                     |
| `CONFIG_NATS_MAX_SUBJECT_LEN`      | 128     | Longest subscription subject                 |
| `CONFIG_NATS_MAX_QUEUE_LEN`        | 32      | Longest queue group name                     |
| `CONFIG_NATS_MAX_PENDING_REQUESTS` | 2       | Requests in flight per client                |
| `CONFIG_NATS_THREAD_STACK_SIZE`    | 2048    | Receive thread stack                         |
| `CONFIG_NATS_THREAD_PRIORITY`      | 7       | Receive thread priority                      |
| `CONFIG_NATS_CONNECT_TIMEOUT_MS`   | 10000   | Time from TCP connect to the first PONG      |
| `CONFIG_NATS_PING_INTERVAL_MS`     | 120000  | Client PING interval                         |
| `CONFIG_NATS_MAX_PINGS_OUT`        | 2       | Unanswered PINGs before reconnecting         |
| `CONFIG_NATS_RECONNECT_MIN_MS`     | 1000    | First reconnect delay                        |
| `CONFIG_NATS_RECONNECT_MAX_MS`     | 30000   | Longest reconnect delay                      |
| `CONFIG_NATS_TLS`                  | n       | Enable `tls://` URLs                         |
| `CONFIG_NATS_HEADERS`              | y       | Enable `HPUB` and `HMSG`                     |
| `CONFIG_NATS_LOG_LEVEL`            | 3       | Log level                                    |

## Using the module

Add it to your west manifest:

```yaml
manifest:
  projects:
    - name: zephyr-nats
      url: https://github.com/simpleiot/zephyr-nats
      revision: main
      path: modules/lib/zephyr-nats
```

Then enable it in `prj.conf`:

```
CONFIG_NETWORKING=y
CONFIG_NET_IPV4=y
CONFIG_NET_TCP=y
CONFIG_NATS=y
```

### TLS

Enable TLS sockets and register the CA that signed the server certificate:

```
CONFIG_NATS_TLS=y
CONFIG_NET_SOCKETS_SOCKOPT_TLS=y
CONFIG_TLS_CREDENTIALS=y
CONFIG_MBEDTLS=y
CONFIG_MBEDTLS_ENABLE_HEAP=y
CONFIG_MBEDTLS_HEAP_SIZE=60000
CONFIG_MBEDTLS_CIPHERSUITE_TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256=y
CONFIG_NATS_THREAD_STACK_SIZE=8192
```

```c
static const sec_tag_t tags[] = {1};

tls_credential_add(1, TLS_CREDENTIAL_CA_CERTIFICATE, ca_der, sizeof(ca_der));

const struct nats_config cfg = {
	.url = "tls://nats.example.com:4222",
	.sec_tags = tags,
	.sec_tag_count = ARRAY_SIZE(tags),
};
```

Zephyr's TLS sockets start TLS when the socket connects, so the client cannot
first read `INFO` in plaintext as other NATS clients do. Configure the server
to expect TLS first:

```
tls {
    cert_file: "server.pem"
    key_file: "server.key"
    handshake_first: true
}
```

## Repository layout

```
zephyr/module.yml     Zephyr module definition
CMakeLists.txt
Kconfig
include/zephyr/net/nats.h
src/                  client, parser, and URL handling
samples/pubsub/       publish and subscribe against a server
samples/request/      request/reply
tests/parser/         ztest suite for the protocol parser
tests/client/         ztest suite against a local nats-server
scripts/              test runners that start nats-server
west.yml              workspace for building the module on its own (CI)
```

## Testing

Most tests run on the host with `native_sim` and a real `nats-server`, so they
need no hardware and run in CI (`.github/workflows/build.yml`). They need a
Zephyr 4.4 workspace; `west.yml` in this repository describes a minimal one.
If the installed Zephyr SDK is older than the workspace expects, set
`ZEPHYR_TOOLCHAIN_VARIANT=host` to build `native_sim` with the host compiler.

### Parser tests

`tests/parser` is a ztest suite with no network. It feeds recorded protocol
streams (`INFO`, `MSG`, `HMSG`, `PING`, `+OK`, `-ERR`, malformed and
oversize lines) to the parser, split at every byte boundary to cover partial
reads. It also checks that an oversize payload is dropped and counted while the
parser stays in sync. URL parsing, subject validation, and header encoding are
tested the same way.

```sh
west twister -T tests -T samples -p native_sim
```

This runs the parser tests and builds the samples and every client test
configuration.

### Client tests

`tests/client` runs on `native_sim` with native host sockets against
`nats-server` instances that a script starts on free ports. Fault tests connect
through a TCP proxy inside the test, which can drop the connection, stop
forwarding the server's replies, or refuse connections. It covers:

- connect, publish, subscribe, wildcards, queue groups, and echo control
- unsubscribe after N messages, request/reply with timeout and no responders,
  headers, flush, and publishing from several threads at once
- messages larger than the transmit buffer, and dropped messages larger than
  the receive buffer
- token and user/password authentication, including rejected credentials
- reconnect after a dropped connection: events, resubscription, and backoff
- stale connection detection when the server stops answering PINGs
- TLS against a server using a CA generated for the run, including a rejected
  hostname. This step is skipped when the installed `nats-server` does not
  support `handshake_first`.

```sh
scripts/run-client-tests.sh
```

### Interoperability

The samples are checked against the `nats` CLI (`nats sub`, `nats pub`,
`nats request`, `nats reply`), so the wire format is verified against the
reference Go client as well as this one.

```sh
scripts/run-interop.sh
```

### Hardware

The pubsub sample runs on a supported board against a server on the LAN. This
measures stack use (`CONFIG_THREAD_ANALYZER`) and RAM and flash size, which set
the Kconfig defaults.

## License

Apache 2.0. See [LICENSE](LICENSE).
