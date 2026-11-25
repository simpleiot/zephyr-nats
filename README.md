# zephyr-nats

A [NATS](https://nats.io) client for [Zephyr RTOS](https://zephyrproject.org),
packaged as a Zephyr module.

It lets an MCU with a TCP connection publish, subscribe, and make requests on a
NATS server, using the same subjects as the rest of the system. It is written
for constrained devices: no heap allocation, fixed buffers sized in Kconfig,
and a single receive thread.

## Status

Design stage. This README describes the intended scope and API; nothing is
implemented yet.

## Features

Core protocol (first release):

- `INFO`, `CONNECT`, `PUB`, `HPUB`, `SUB`, `UNSUB`, `MSG`, `HMSG`, `PING`,
  `PONG`, `+OK`, `-ERR`
- Message headers (NATS/1.0)
- Request/reply with a per-connection inbox
- Queue groups
- Server and client PINGs, with a configurable stale-connection limit
- Automatic reconnect with backoff, resubscribing on reconnect
- TLS through Zephyr's TLS sockets (mbed TLS), with server verification
- Authentication: token, user and password

Later:

- NKey and JWT (`.creds`) authentication, which need Ed25519 signatures
- JetStream: publish with acknowledgment, and pull consumers (`Fetch`)
- Multiple servers in a cluster, from the `connect_urls` in `INFO`

Not planned:

- Push consumers, the key-value and object store APIs, and WebSocket transport

## Design

- **No heap.** The application provides the client and subscription structs,
  usually as statics. Receive and transmit buffers are fixed, sized by
  `CONFIG_NATS_RX_BUF_SIZE` and `CONFIG_NATS_TX_BUF_SIZE`. A message larger than
  the receive buffer is dropped and counted, and the connection stays up.
- **One thread per client.** A receive thread reads the socket, parses
  protocol lines, answers PINGs, and calls subscription callbacks. Callbacks
  run in that thread, so they should return quickly or hand work to another
  thread (a work queue or zbus channel).
- **Thread-safe publish.** `nats_publish()` and `nats_request()` can be called
  from any thread. Writes are serialized with a mutex.
- **Incremental parser.** The protocol parser is a state machine that does not
  depend on sockets, so it can be tested on its own and fed partial reads.
- **Zephyr networking.** The client uses the BSD socket API (`zsock_*`) and
  `getaddrinfo()`, so it works over Ethernet, Wi-Fi, cellular, and native_sim.

## API sketch

```c
#include <zephyr/net/nats.h>

static struct nats_client client;
static struct nats_sub cmd_sub;

static void on_cmd(struct nats_client *c, const struct nats_msg *msg,
                   void *user_data)
{
	printk("%.*s: %.*s\n", msg->subject_len, msg->subject,
	       msg->data_len, (const char *)msg->data);

	if (msg->reply_len > 0) {
		nats_respond(c, msg, "ok", 2);
	}
}

int main(void)
{
	const struct nats_config cfg = {
		.url = "nats://demo.nats.io:4222",
		.name = "sensor-1",
		.event_cb = NULL,
	};

	nats_client_init(&client, &cfg);
	nats_connect(&client);

	nats_subscribe(&client, &cmd_sub, "dev.sensor-1.cmd", NULL, on_cmd, NULL);
	nats_publish(&client, "dev.sensor-1.temp", "21.5", 4);

	return 0;
}
```

Main functions:

| Function                  | Purpose                                                   |
| ------------------------- | --------------------------------------------------------- |
| `nats_client_init()`      | Set up a client from a config                             |
| `nats_connect()`          | Connect and start the receive thread                      |
| `nats_disconnect()`       | Drain, close, and stop reconnecting                       |
| `nats_publish()`          | Publish a message, with optional reply subject            |
| `nats_publish_hdr()`      | Publish a message with headers                            |
| `nats_subscribe()`        | Subscribe, with optional queue group                      |
| `nats_unsubscribe()`      | Unsubscribe, optionally after N messages                  |
| `nats_request()`          | Publish and wait for one reply, with a timeout            |
| `nats_respond()`          | Reply to a received message                               |
| `nats_flush()`            | Send a PING and wait for the PONG                         |

Connection events (connected, disconnected, reconnected, error) are reported
through `event_cb`.

## Configuration

| Kconfig option                  | Default | Description                                   |
| ------------------------------- | ------- | --------------------------------------------- |
| `CONFIG_NATS`                   | n       | Enable the NATS client                        |
| `CONFIG_NATS_RX_BUF_SIZE`       | 1024    | Largest protocol line plus payload received   |
| `CONFIG_NATS_TX_BUF_SIZE`       | 512     | Buffer for outgoing protocol lines            |
| `CONFIG_NATS_MAX_SUBS`          | 8       | Subscriptions per client                      |
| `CONFIG_NATS_THREAD_STACK_SIZE` | 2048    | Receive thread stack                          |
| `CONFIG_NATS_THREAD_PRIORITY`   | 7       | Receive thread priority                       |
| `CONFIG_NATS_PING_INTERVAL_MS`  | 120000  | Client PING interval                          |
| `CONFIG_NATS_MAX_PINGS_OUT`     | 2       | Unanswered PINGs before reconnecting          |
| `CONFIG_NATS_RECONNECT_MIN_MS`  | 1000    | First reconnect delay                         |
| `CONFIG_NATS_RECONNECT_MAX_MS`  | 30000   | Longest reconnect delay                       |
| `CONFIG_NATS_TLS`               | n       | Enable `tls://` URLs                          |
| `CONFIG_NATS_HEADERS`           | y       | Enable `HPUB` and `HMSG`                      |
| `CONFIG_NATS_LOG_LEVEL`         | 3       | Log level                                     |

## Using the module

Add it to your west manifest:

```yaml
manifest:
  projects:
    - name: zephyr-nats
      url: https://github.com/simpleiot/zephyr-nats
      revision: main
      path: modules/lib/nats
```

Then enable it in `prj.conf`:

```
CONFIG_NETWORKING=y
CONFIG_NET_SOCKETS=y
CONFIG_NATS=y
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
```

## Testing

The parser tests run on `native_sim` with no network:

```sh
west twister -T tests/parser -p native_sim
```

The client tests run on `native_sim` with native sockets and need a
`nats-server` listening on `localhost:4222`:

```sh
nats-server &
west twister -T tests/client -p native_sim
```

## License

Apache 2.0. See [LICENSE](LICENSE).
