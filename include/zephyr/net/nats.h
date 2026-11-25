/*
 * Copyright (c) 2026 Cliff Brake
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * @file
 * @brief NATS client
 *
 * A client for the NATS core protocol. The application owns every struct, so
 * the client allocates nothing at run time. Each client runs one receive
 * thread, which also calls subscription and event callbacks.
 */

#ifndef ZEPHYR_INCLUDE_NET_NATS_H_
#define ZEPHYR_INCLUDE_NET_NATS_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <zephyr/kernel.h>

#ifdef CONFIG_NATS_TLS
#include <zephyr/net/tls_credentials.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif

struct nats_client;
struct nats_sub;

/** A received message. Valid only for the duration of the callback. */
struct nats_msg {
	/** Subject, NUL-terminated */
	const char *subject;
	size_t subject_len;
	/** Reply subject, NUL-terminated; empty when reply_len is 0 */
	const char *reply;
	size_t reply_len;
	/** Header block, or NULL when the message has no headers */
	const uint8_t *hdr;
	size_t hdr_len;
	/** Status code from the header block, such as 503 for no responders; 0 if none */
	uint16_t status;
	/** Payload. data[data_len] is always '\0', so text payloads can be used as strings. */
	const uint8_t *data;
	size_t data_len;
	/** Subscription the message arrived on */
	struct nats_sub *sub;
};

/** A header to send with nats_publish_hdr(). */
struct nats_hdr {
	const char *key;
	const char *value;
};

/** Called for each message on a subscription. */
typedef void (*nats_msg_cb_t)(struct nats_client *client, const struct nats_msg *msg,
			      void *user_data);

enum nats_event {
	/** First connection to the server is ready */
	NATS_EVENT_CONNECTED,
	/** Connection was lost or closed; the client retries unless it was closed */
	NATS_EVENT_DISCONNECTED,
	/** Connection is ready again after a disconnect, with subscriptions restored */
	NATS_EVENT_RECONNECTED,
	/** Server sent -ERR; the text is passed in err */
	NATS_EVENT_ERROR,
};

/** Called on connection state changes and server errors. */
typedef void (*nats_event_cb_t)(struct nats_client *client, enum nats_event event,
				const char *err, void *user_data);

struct nats_config {
	/** Server URL: nats://[user:pass@|token@]host[:port] or tls://... */
	const char *url;
	/** Client name shown by the server, or NULL */
	const char *name;
	/** User and password; override any in the URL */
	const char *user;
	const char *pass;
	/** Authentication token; overrides any in the URL */
	const char *token;
	/** Do not deliver messages this client publishes to its own subscriptions */
	bool no_echo;
	nats_event_cb_t event_cb;
	void *user_data;
#ifdef CONFIG_NATS_TLS
	/** Credentials registered with tls_credential_add() */
	const sec_tag_t *sec_tags;
	size_t sec_tag_count;
	/** Name to verify in the server certificate; defaults to the URL host */
	const char *tls_hostname;
#endif
};

/** Counters since nats_client_init(). */
struct nats_stats {
	uint32_t in_msgs;
	uint32_t out_msgs;
	uint32_t in_bytes;
	uint32_t out_bytes;
	/** Messages dropped because they did not fit in CONFIG_NATS_RX_BUF_SIZE */
	uint32_t dropped;
	uint32_t reconnects;
};

/** @cond INTERNAL_HIDDEN */

struct nats_sub {
	nats_msg_cb_t cb;
	void *user_data;
	uint32_t sid;
	uint32_t max_msgs;
	uint32_t delivered;
	char subject[CONFIG_NATS_MAX_SUBJECT_LEN];
	char queue[CONFIG_NATS_MAX_QUEUE_LEN];
};

struct nats_pending {
	struct k_sem done;
	uint32_t token;
	bool active;
	int result;
	uint8_t *buf;
	size_t size;
	size_t len;
};

/* Receive buffer state; see src/nats_parser.h */
struct nats_parser {
	uint8_t *buf;
	size_t size;
	size_t len;
	size_t consumed;
	size_t skip;
	uint32_t dropped;
};

enum nats_state {
	NATS_STATE_IDLE,
	NATS_STATE_CONNECTING,
	NATS_STATE_HANDSHAKE,
	NATS_STATE_CONNECTED,
};

struct nats_client {
	struct nats_config cfg;

	/* Protects state, subscriptions, pending requests, and counters */
	struct k_mutex lock;
	struct k_condvar cond;
	/* Serializes writes to the socket; taken after lock, never before */
	struct k_mutex tx_lock;

	int sock;
	enum nats_state state;
	bool running;
	bool stop;
	bool ever_connected;
	bool subs_sent;
	uint32_t conn_gen;
	uint32_t pings_sent;
	uint32_t pongs_recv;
	uint32_t pings_out;
	uint32_t max_payload;
	bool server_headers;

	struct nats_sub *subs[CONFIG_NATS_MAX_SUBS];
	uint32_t next_sid;

	char inbox[32];
	struct nats_sub inbox_sub;
	struct nats_pending pending[CONFIG_NATS_MAX_PENDING_REQUESTS];
	uint32_t next_token;

	struct nats_stats stats;

	struct k_thread thread;

	K_KERNEL_STACK_MEMBER(stack, CONFIG_NATS_THREAD_STACK_SIZE);

	uint8_t rx_buf[CONFIG_NATS_RX_BUF_SIZE];
	uint8_t tx_buf[CONFIG_NATS_TX_BUF_SIZE];
	struct nats_parser parser;
};

/** @endcond */

/**
 * @brief Initialize a client.
 *
 * The config is copied, but the strings it points to must stay valid for the
 * life of the client.
 *
 * @return 0, or -EINVAL if the URL cannot be parsed.
 */
int nats_client_init(struct nats_client *client, const struct nats_config *cfg);

/**
 * @brief Start connecting and wait for the connection to be ready.
 *
 * Starts the receive thread, which keeps trying to connect, and reconnects
 * after any failure until nats_disconnect() is called.
 *
 * @param timeout How long to wait for the first connection.
 * @return 0 when connected, -EAGAIN if the timeout passed first (the client
 *         keeps trying in the background), or -EALREADY if already started.
 */
int nats_connect(struct nats_client *client, k_timeout_t timeout);

/**
 * @brief Flush pending data, close the connection, and stop reconnecting.
 *
 * Must not be called from a callback.
 */
int nats_disconnect(struct nats_client *client);

/** @brief Return true if the client is connected and ready. */
bool nats_is_connected(struct nats_client *client);

/**
 * @brief Publish a message.
 *
 * Messages are not buffered while disconnected.
 *
 * @return 0, -ENOTCONN if not connected, -EINVAL for an invalid subject,
 *         -EMSGSIZE if the payload exceeds the server limit, or a socket error.
 */
int nats_publish(struct nats_client *client, const char *subject, const void *data, size_t len);

/** @brief Publish a message with a reply subject. */
int nats_publish_reply(struct nats_client *client, const char *subject, const char *reply,
		       const void *data, size_t len);

/**
 * @brief Publish a message with headers.
 *
 * @param reply Reply subject, or NULL.
 * @return As nats_publish(), or -ENOTSUP if the server does not support headers.
 */
int nats_publish_hdr(struct nats_client *client, const char *subject, const char *reply,
		     const struct nats_hdr *hdrs, size_t hdr_count, const void *data, size_t len);

/**
 * @brief Subscribe to a subject.
 *
 * The subscription struct must stay valid until it is unsubscribed. While
 * disconnected, the subscription is recorded and sent on connect.
 *
 * @param queue Queue group, or NULL.
 * @return 0, -EINVAL for an invalid subject, -ENOMEM if CONFIG_NATS_MAX_SUBS
 *         subscriptions are active, or -EALREADY if sub is already in use.
 */
int nats_subscribe(struct nats_client *client, struct nats_sub *sub, const char *subject,
		   const char *queue, nats_msg_cb_t cb, void *user_data);

/**
 * @brief Unsubscribe.
 *
 * @param max_msgs 0 to unsubscribe now, or the total number of messages to
 *        deliver before unsubscribing automatically.
 */
int nats_unsubscribe(struct nats_client *client, struct nats_sub *sub, uint32_t max_msgs);

/**
 * @brief Publish a request and wait for one reply.
 *
 * Must not be called from a callback.
 *
 * @param reply_buf Buffer for the reply payload. It is NUL-terminated when
 *        it has room.
 * @param reply_len Set to the reply length, which may exceed reply_size.
 * @return 0, -ETIMEDOUT, -EHOSTUNREACH if no subscriber is listening,
 *         -ENOBUFS if the reply was truncated, -EBUSY if
 *         CONFIG_NATS_MAX_PENDING_REQUESTS requests are in flight, or as
 *         nats_publish().
 */
int nats_request(struct nats_client *client, const char *subject, const void *data, size_t len,
		 void *reply_buf, size_t reply_size, size_t *reply_len, k_timeout_t timeout);

/** @brief Publish to the reply subject of a received message. */
int nats_respond(struct nats_client *client, const struct nats_msg *msg, const void *data,
		 size_t len);

/**
 * @brief Send a PING and wait for the server's PONG.
 *
 * When it returns 0, the server has processed everything published before.
 * Must not be called from a callback.
 */
int nats_flush(struct nats_client *client, k_timeout_t timeout);

/**
 * @brief Find a header in a received message, matching the key exactly.
 *
 * @return 0 with *val and *val_len set (not NUL-terminated), or -ENOENT.
 */
int nats_msg_header(const struct nats_msg *msg, const char *key, const char **val,
		    size_t *val_len);

/** @brief Copy the client's counters. */
void nats_client_stats(struct nats_client *client, struct nats_stats *stats);

#ifdef __cplusplus
}
#endif

#endif /* ZEPHYR_INCLUDE_NET_NATS_H_ */
