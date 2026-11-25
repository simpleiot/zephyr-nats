/*
 * Copyright (c) 2026 Cliff Brake
 * SPDX-License-Identifier: Apache-2.0
 */

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>
#include <zephyr/net/nats.h>
#include <zephyr/net/socket.h>
#include <zephyr/random/random.h>
#include <zephyr/sys/util.h>

#include "nats_parser.h"
#include "nats_util.h"

LOG_MODULE_REGISTER(net_nats, CONFIG_NATS_LOG_LEVEL);

#define CLIENT_LANG    "zephyr"
#define CLIENT_VERSION "0.1.0"

#define INBOX_SID 1
#define FIRST_SID 2

#define STATUS_NO_RESPONDERS 503

static bool in_rx_thread(struct nats_client *c)
{
	return k_current_get() == &c->thread;
}

static void emit(struct nats_client *c, enum nats_event event, const char *err)
{
	if (c->cfg.event_cb != NULL) {
		c->cfg.event_cb(c, event, err, c->cfg.user_data);
	}
}

/* Socket writes. The caller holds tx_lock. */

static int tx_raw(struct nats_client *c, const void *buf, size_t len)
{
	const uint8_t *p = buf;

	while (len > 0) {
		ssize_t n = zsock_send(c->sock, p, len, 0);

		if (n < 0) {
			return -errno;
		}
		p += n;
		len -= n;
	}

	return 0;
}

static int tx_line(struct nats_client *c, const char *fmt, ...)
{
	va_list ap;
	int n;

	if (c->sock < 0) {
		return -ENOTCONN;
	}

	va_start(ap, fmt);
	n = vsnprintk((char *)c->tx_buf, sizeof(c->tx_buf), fmt, ap);
	va_end(ap);

	if (n < 0 || n >= (int)sizeof(c->tx_buf)) {
		return -ENOMEM;
	}

	return tx_raw(c, c->tx_buf, n);
}

/* Subscriptions. The caller holds lock. */

static int find_sub(struct nats_client *c, struct nats_sub *sub)
{
	for (int i = 0; i < ARRAY_SIZE(c->subs); i++) {
		if (c->subs[i] == sub) {
			return i;
		}
	}

	return -ENOENT;
}

static int send_sub(struct nats_client *c, struct nats_sub *sub)
{
	int ret;

	k_mutex_lock(&c->tx_lock, K_FOREVER);
	if (sub->queue[0] != '\0') {
		ret = tx_line(c, "SUB %s %s %u\r\n", sub->subject, sub->queue, sub->sid);
	} else {
		ret = tx_line(c, "SUB %s %u\r\n", sub->subject, sub->sid);
	}
	if (ret == 0 && sub->max_msgs > sub->delivered) {
		ret = tx_line(c, "UNSUB %u %u\r\n", sub->sid, sub->max_msgs - sub->delivered);
	}
	k_mutex_unlock(&c->tx_lock);

	return ret;
}

/* Fail every request in flight, for example when the connection drops. */
static void fail_pending(struct nats_client *c, int result)
{
	for (int i = 0; i < ARRAY_SIZE(c->pending); i++) {
		struct nats_pending *p = &c->pending[i];

		if (p->active) {
			p->active = false;
			p->result = result;
			k_sem_give(&p->done);
		}
	}
}

static int set_state(struct nats_client *c, enum nats_state state)
{
	k_mutex_lock(&c->lock, K_FOREVER);
	c->state = state;
	k_condvar_broadcast(&c->cond);
	k_mutex_unlock(&c->lock);

	return 0;
}

/* Connection setup */

static int open_socket(struct nats_client *c, const struct nats_url *url)
{
	struct net_sockaddr_storage addr = {0};
	net_socklen_t addrlen;
	int proto = IS_ENABLED(CONFIG_NATS_TLS) && url->tls ? NET_IPPROTO_TLS_1_2
							     : NET_IPPROTO_TCP;
	int sock;
	int ret;

	struct net_sockaddr_in *sin = (struct net_sockaddr_in *)&addr;
	struct net_sockaddr_in6 *sin6 = (struct net_sockaddr_in6 *)&addr;

	/* Numeric addresses skip getaddrinfo(), which allocates from the system heap. */
	if (zsock_inet_pton(NET_AF_INET, url->host, &sin->sin_addr) == 1) {
		sin->sin_family = NET_AF_INET;
		sin->sin_port = net_htons(url->port);
		addrlen = sizeof(*sin);
	} else if (zsock_inet_pton(NET_AF_INET6, url->host, &sin6->sin6_addr) == 1) {
		sin6->sin6_family = NET_AF_INET6;
		sin6->sin6_port = net_htons(url->port);
		addrlen = sizeof(*sin6);
	} else {
#if defined(CONFIG_DNS_RESOLVER) || defined(CONFIG_NET_SOCKETS_OFFLOAD)
		struct zsock_addrinfo *res = NULL;
		struct zsock_addrinfo hints = {
			.ai_family = NET_AF_UNSPEC,
			.ai_socktype = NET_SOCK_STREAM,
		};
		char port[6];

		snprintk(port, sizeof(port), "%u", url->port);
		ret = zsock_getaddrinfo(url->host, port, &hints, &res);
		if (ret != 0 || res == NULL) {
			LOG_WRN("cannot resolve %s (%d)", url->host, ret);
			return -EHOSTUNREACH;
		}
		memcpy(&addr, res->ai_addr, MIN(res->ai_addrlen, sizeof(addr)));
		addrlen = res->ai_addrlen;
		zsock_freeaddrinfo(res);
#else
		LOG_ERR("resolving %s needs CONFIG_DNS_RESOLVER", url->host);
		return -EHOSTUNREACH;
#endif
	}

	sock = zsock_socket(addr.ss_family, NET_SOCK_STREAM, proto);
	if (sock < 0) {
		return -errno;
	}

#ifdef CONFIG_NATS_TLS
	if (url->tls) {
		const char *hostname = c->cfg.tls_hostname ? c->cfg.tls_hostname : url->host;

		if (c->cfg.sec_tag_count > 0 &&
		    zsock_setsockopt(sock, ZSOCK_SOL_TLS, ZSOCK_TLS_SEC_TAG_LIST, c->cfg.sec_tags,
				     c->cfg.sec_tag_count * sizeof(sec_tag_t)) < 0) {
			ret = -errno;
			goto fail;
		}
		if (zsock_setsockopt(sock, ZSOCK_SOL_TLS, ZSOCK_TLS_HOSTNAME, hostname,
				     strlen(hostname) + 1) < 0) {
			ret = -errno;
			goto fail;
		}
	}
#endif

	if (zsock_connect(sock, (struct net_sockaddr *)&addr, addrlen) < 0) {
		ret = -errno;
		LOG_WRN("connect to %s:%u failed (%d)", url->host, url->port, ret);
		goto fail;
	}

	return sock;

fail:
	zsock_close(sock);
	return ret;
}

/* Append a JSON field to the CONNECT line being built in tx_buf. */
static int json_field(struct nats_client *c, size_t *n, const char *key, const char *val,
		      size_t len)
{
	int ret;

	ret = snprintk((char *)c->tx_buf + *n, sizeof(c->tx_buf) - *n, ",\"%s\":\"", key);
	if (ret < 0 || *n + ret >= sizeof(c->tx_buf)) {
		return -ENOMEM;
	}
	*n += ret;

	ret = nats_json_escape((char *)c->tx_buf + *n, sizeof(c->tx_buf) - *n, val, len);
	if (ret < 0) {
		return ret;
	}
	*n += ret;

	if (*n + 1 >= sizeof(c->tx_buf)) {
		return -ENOMEM;
	}
	c->tx_buf[(*n)++] = '"';

	return 0;
}

static int send_connect(struct nats_client *c, const struct nats_url *url)
{
	const struct nats_config *cfg = &c->cfg;
	bool headers = IS_ENABLED(CONFIG_NATS_HEADERS) && c->server_headers;
	size_t n;
	int ret;

	ret = snprintk((char *)c->tx_buf, sizeof(c->tx_buf),
		       "CONNECT {\"verbose\":false,\"pedantic\":false,\"tls_required\":%s,"
		       "\"lang\":\"" CLIENT_LANG "\",\"version\":\"" CLIENT_VERSION "\","
		       "\"protocol\":1,\"echo\":%s,\"headers\":%s,\"no_responders\":%s",
		       url->tls ? "true" : "false", cfg->no_echo ? "false" : "true",
		       headers ? "true" : "false", headers ? "true" : "false");
	if (ret < 0 || ret >= sizeof(c->tx_buf)) {
		return -ENOMEM;
	}
	n = ret;

	if (cfg->name != NULL) {
		ret = json_field(c, &n, "name", cfg->name, strlen(cfg->name));
	}

	if (ret >= 0 && cfg->token != NULL) {
		ret = json_field(c, &n, "auth_token", cfg->token, strlen(cfg->token));
	} else if (ret >= 0 && cfg->user != NULL) {
		ret = json_field(c, &n, "user", cfg->user, strlen(cfg->user));
		if (ret >= 0 && cfg->pass != NULL) {
			ret = json_field(c, &n, "pass", cfg->pass, strlen(cfg->pass));
		}
	} else if (ret >= 0 && url->token != NULL) {
		ret = json_field(c, &n, "auth_token", url->token, url->token_len);
	} else if (ret >= 0 && url->user != NULL) {
		ret = json_field(c, &n, "user", url->user, url->user_len);
		if (ret >= 0) {
			ret = json_field(c, &n, "pass", url->pass, url->pass_len);
		}
	}

	if (ret < 0 || n + 3 > sizeof(c->tx_buf)) {
		LOG_ERR("CONNECT does not fit in CONFIG_NATS_TX_BUF_SIZE");
		return -ENOMEM;
	}

	memcpy(c->tx_buf + n, "}\r\n", 3);
	n += 3;

	k_mutex_lock(&c->tx_lock, K_FOREVER);
	ret = tx_raw(c, c->tx_buf, n);
	k_mutex_unlock(&c->tx_lock);

	return ret;
}

static int send_ping(struct nats_client *c, uint32_t *target)
{
	int ret;

	k_mutex_lock(&c->tx_lock, K_FOREVER);
	ret = tx_line(c, "PING\r\n");
	if (ret == 0) {
		c->pings_sent++;
		if (target != NULL) {
			*target = c->pings_sent;
		}
	}
	k_mutex_unlock(&c->tx_lock);

	return ret;
}

/* Send CONNECT, restore subscriptions, and PING to confirm the server accepted them. */
static int handshake(struct nats_client *c, const struct nats_url *url, const struct nats_op *op)
{
	uint32_t max_payload;
	bool flag;
	int ret;

	if (nats_json_get_bool(op->args, op->args_len, "tls_required", &flag) == 0 && flag &&
	    !url->tls) {
		LOG_ERR("server requires TLS; use a tls:// URL");
		return -EPROTONOSUPPORT;
	}

	k_mutex_lock(&c->tx_lock, K_FOREVER);
	c->max_payload = UINT32_MAX;
	if (nats_json_get_uint(op->args, op->args_len, "max_payload", &max_payload) == 0) {
		c->max_payload = max_payload;
	}
	c->server_headers = false;
	if (nats_json_get_bool(op->args, op->args_len, "headers", &flag) == 0) {
		c->server_headers = flag;
	}
	k_mutex_unlock(&c->tx_lock);

	ret = send_connect(c, url);
	if (ret < 0) {
		return ret;
	}

	k_mutex_lock(&c->lock, K_FOREVER);
	ret = send_sub(c, &c->inbox_sub);
	for (int i = 0; ret == 0 && i < ARRAY_SIZE(c->subs); i++) {
		if (c->subs[i] != NULL) {
			ret = send_sub(c, c->subs[i]);
		}
	}
	if (ret == 0) {
		c->subs_sent = true;
	}
	k_mutex_unlock(&c->lock);

	if (ret == 0) {
		ret = send_ping(c, NULL);
	}

	return ret;
}

/* Receiving */

static void handle_reply(struct nats_client *c, const struct nats_msg *msg)
{
	const char *dot = strrchr(msg->subject, '.');
	uint32_t token;

	if (dot == NULL) {
		return;
	}

	token = strtoul(dot + 1, NULL, 10);

	k_mutex_lock(&c->lock, K_FOREVER);
	for (int i = 0; i < ARRAY_SIZE(c->pending); i++) {
		struct nats_pending *p = &c->pending[i];

		if (!p->active || p->token != token) {
			continue;
		}

		if (msg->status == STATUS_NO_RESPONDERS && msg->data_len == 0) {
			p->result = -EHOSTUNREACH;
			p->len = 0;
		} else {
			size_t n = MIN(msg->data_len, p->size);

			memcpy(p->buf, msg->data, n);
			if (n < p->size) {
				p->buf[n] = '\0';
			}
			p->len = msg->data_len;
			p->result = n < msg->data_len ? -ENOBUFS : 0;
		}
		p->active = false;
		k_sem_give(&p->done);
		break;
	}
	k_mutex_unlock(&c->lock);
}

static void dispatch(struct nats_client *c, const struct nats_op *op)
{
	struct nats_sub *sub = NULL;
	nats_msg_cb_t cb = NULL;
	void *user_data = NULL;
	struct nats_msg msg = {
		.subject = op->subject,
		.subject_len = op->subject_len,
		.reply = op->reply,
		.reply_len = op->reply_len,
		.hdr = op->hdr,
		.hdr_len = op->hdr_len,
		.data = op->data,
		.data_len = op->data_len,
	};

	if (op->hdr != NULL) {
		msg.status = nats_hdr_status(op->hdr, op->hdr_len);
	}

	k_mutex_lock(&c->lock, K_FOREVER);
	c->stats.in_msgs++;
	c->stats.in_bytes += op->data_len;

	if (op->sid == INBOX_SID) {
		k_mutex_unlock(&c->lock);
		msg.sub = &c->inbox_sub;
		handle_reply(c, &msg);
		return;
	}

	for (int i = 0; i < ARRAY_SIZE(c->subs); i++) {
		if (c->subs[i] != NULL && c->subs[i]->sid == op->sid) {
			sub = c->subs[i];
			sub->delivered++;
			cb = sub->cb;
			user_data = sub->user_data;
			if (sub->max_msgs > 0 && sub->delivered >= sub->max_msgs) {
				c->subs[i] = NULL;
			}
			break;
		}
	}
	k_mutex_unlock(&c->lock);

	if (cb != NULL) {
		msg.sub = sub;
		cb(c, &msg, user_data);
	}
}

static int handle_op(struct nats_client *c, const struct nats_url *url, const struct nats_op *op,
		     bool *info_seen)
{
	bool ldm;
	int ret;

	switch (op->type) {
	case NATS_OP_PING:
		k_mutex_lock(&c->tx_lock, K_FOREVER);
		ret = tx_line(c, "PONG\r\n");
		k_mutex_unlock(&c->tx_lock);
		return ret;

	case NATS_OP_PONG:
		c->pings_out = 0;
		k_mutex_lock(&c->lock, K_FOREVER);
		c->pongs_recv++;
		if (c->state == NATS_STATE_HANDSHAKE) {
			c->state = NATS_STATE_CONNECTED;
		}
		k_condvar_broadcast(&c->cond);
		k_mutex_unlock(&c->lock);
		return 0;

	case NATS_OP_INFO:
		if (!*info_seen) {
			*info_seen = true;
			return handshake(c, url, op);
		}
		if (nats_json_get_bool(op->args, op->args_len, "ldm", &ldm) == 0 && ldm) {
			LOG_INF("server is entering lame duck mode");
		}
		return 0;

	case NATS_OP_MSG:
	case NATS_OP_HMSG:
		dispatch(c, op);
		return 0;

	case NATS_OP_DROPPED:
		LOG_WRN("dropped %zu byte message on %s: larger than CONFIG_NATS_RX_BUF_SIZE",
			op->data_len, op->subject);
		k_mutex_lock(&c->lock, K_FOREVER);
		c->stats.dropped++;
		k_mutex_unlock(&c->lock);
		return 0;

	case NATS_OP_ERR:
		LOG_WRN("server error: %s", op->args);
		emit(c, NATS_EVENT_ERROR, op->args);
		/* An error before the handshake completes, such as failed authorization, is fatal. */
		return c->state == NATS_STATE_HANDSHAKE ? -EACCES : 0;

	case NATS_OP_OK:
	default:
		return 0;
	}
}

static int64_t ms_until(int64_t deadline)
{
	return MAX(deadline - k_uptime_get(), 0);
}

static bool should_stop(struct nats_client *c)
{
	bool stop;

	k_mutex_lock(&c->lock, K_FOREVER);
	stop = c->stop;
	k_mutex_unlock(&c->lock);

	return stop;
}

/* Run one connection until it fails or the client stops. */
static int run_connection(struct nats_client *c, const struct nats_url *url, bool *connected)
{
	struct zsock_pollfd pfd = {.events = ZSOCK_POLLIN};
	int64_t deadline = k_uptime_get() + CONFIG_NATS_CONNECT_TIMEOUT_MS;
	bool info_seen = false;
	struct nats_op op;
	int sock;
	int ret;

	sock = open_socket(c, url);
	if (sock < 0) {
		return sock;
	}

	k_mutex_lock(&c->lock, K_FOREVER);
	k_mutex_lock(&c->tx_lock, K_FOREVER);
	c->sock = sock;
	c->pings_sent = 0;
	c->pongs_recv = 0;
	c->pings_out = 0;
	c->state = NATS_STATE_HANDSHAKE;
	k_mutex_unlock(&c->tx_lock);
	k_mutex_unlock(&c->lock);

	nats_parser_reset(&c->parser);
	pfd.fd = sock;

	for (;;) {
		uint8_t *space;
		size_t avail;
		ssize_t n;

		if (should_stop(c)) {
			ret = 0;
			break;
		}

		if (!*connected && c->state == NATS_STATE_CONNECTED) {
			*connected = true;
			LOG_INF("connected to %s:%u", url->host, url->port);
			if (c->ever_connected) {
				k_mutex_lock(&c->lock, K_FOREVER);
				c->stats.reconnects++;
				k_mutex_unlock(&c->lock);
			}
			emit(c, c->ever_connected ? NATS_EVENT_RECONNECTED : NATS_EVENT_CONNECTED,
			     NULL);
			c->ever_connected = true;
			deadline = k_uptime_get() + CONFIG_NATS_PING_INTERVAL_MS;
		}

		ret = zsock_poll(&pfd, 1, (int)MIN(ms_until(deadline), INT32_MAX));
		if (ret < 0) {
			ret = -errno;
			break;
		}

		if (ret == 0) {
			if (!*connected) {
				LOG_WRN("handshake timed out");
				ret = -ETIMEDOUT;
				break;
			}
			if (c->pings_out >= CONFIG_NATS_MAX_PINGS_OUT) {
				LOG_WRN("no PONG from server; reconnecting");
				ret = -ETIMEDOUT;
				break;
			}
			ret = send_ping(c, NULL);
			if (ret < 0) {
				break;
			}
			c->pings_out++;
			deadline = k_uptime_get() + CONFIG_NATS_PING_INTERVAL_MS;
			continue;
		}

		/*
		 * On POLLERR or POLLHUP, read anyway: data such as an -ERR sent just
		 * before the server closed may still be queued, and recv() reports the
		 * error once it is drained.
		 */
		avail = nats_parser_space(&c->parser, &space);
		__ASSERT_NO_MSG(avail > 0);

		n = zsock_recv(sock, space, avail, ZSOCK_MSG_DONTWAIT);
		if (n == 0) {
			ret = -ECONNRESET;
			break;
		}
		if (n < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK) {
				continue;
			}
			ret = -errno;
			break;
		}

		nats_parser_commit(&c->parser, n);

		while ((ret = nats_parser_next(&c->parser, &op)) > 0) {
			ret = handle_op(c, url, &op, &info_seen);
			if (ret < 0) {
				break;
			}
		}

		if (ret < 0) {
			if (ret == -EMSGSIZE || ret == -EBADMSG) {
				LOG_ERR("protocol error (%d)", ret);
			}
			break;
		}
	}

	k_mutex_lock(&c->lock, K_FOREVER);
	c->state = NATS_STATE_CONNECTING;
	c->subs_sent = false;
	c->conn_gen++;
	fail_pending(c, -ENOTCONN);
	k_condvar_broadcast(&c->cond);
	k_mutex_unlock(&c->lock);

	zsock_shutdown(sock, ZSOCK_SHUT_RDWR);

	k_mutex_lock(&c->tx_lock, K_FOREVER);
	zsock_close(sock);
	c->sock = -1;
	k_mutex_unlock(&c->tx_lock);

	if (*connected) {
		LOG_INF("disconnected (%d)", ret);
		emit(c, NATS_EVENT_DISCONNECTED, NULL);
	}

	return ret;
}

/* Wait for ms or until the client is stopped. Returns true if stopped. */
static bool wait_or_stop(struct nats_client *c, uint32_t ms)
{
	k_timepoint_t end = sys_timepoint_calc(K_MSEC(ms));
	bool stop;

	k_mutex_lock(&c->lock, K_FOREVER);
	while (!c->stop && !sys_timepoint_expired(end)) {
		k_condvar_wait(&c->cond, &c->lock, sys_timepoint_timeout(end));
	}
	stop = c->stop;
	k_mutex_unlock(&c->lock);

	return stop;
}

static void client_thread(void *p1, void *p2, void *p3)
{
	struct nats_client *c = p1;
	uint32_t backoff = CONFIG_NATS_RECONNECT_MIN_MS;
	struct nats_url url;

	ARG_UNUSED(p2);
	ARG_UNUSED(p3);

	/* nats_client_init() already checked the URL. */
	(void)nats_url_parse(c->cfg.url, &url);

	for (;;) {
		bool connected = false;
		uint32_t delay;

		set_state(c, NATS_STATE_CONNECTING);
		(void)run_connection(c, &url, &connected);

		if (connected) {
			backoff = CONFIG_NATS_RECONNECT_MIN_MS;
		}

		if (should_stop(c)) {
			break;
		}

		/* Spread reconnects so a fleet does not return all at once. */
		delay = backoff / 2 + sys_rand32_get() % (backoff / 2 + 1);
		LOG_DBG("reconnecting in %u ms", delay);
		if (wait_or_stop(c, delay)) {
			break;
		}
		backoff = MIN(backoff * 2, CONFIG_NATS_RECONNECT_MAX_MS);
	}

	set_state(c, NATS_STATE_IDLE);
}

/* Public API */

int nats_client_init(struct nats_client *c, const struct nats_config *cfg)
{
	static const char alphabet[] =
		"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
	struct nats_url url;
	size_t n;

	if (c == NULL || cfg == NULL || nats_url_parse(cfg->url, &url) < 0) {
		return -EINVAL;
	}

	if (url.tls && !IS_ENABLED(CONFIG_NATS_TLS)) {
		LOG_ERR("tls:// URL needs CONFIG_NATS_TLS");
		return -ENOTSUP;
	}

	memset(c, 0, sizeof(*c));
	c->cfg = *cfg;
	c->sock = -1;
	c->state = NATS_STATE_IDLE;
	c->next_sid = FIRST_SID;
	c->max_payload = UINT32_MAX;

	k_mutex_init(&c->lock);
	k_mutex_init(&c->tx_lock);
	k_condvar_init(&c->cond);

	for (int i = 0; i < ARRAY_SIZE(c->pending); i++) {
		k_sem_init(&c->pending[i].done, 0, 1);
	}

	nats_parser_init(&c->parser, c->rx_buf, sizeof(c->rx_buf));

	n = snprintk(c->inbox, sizeof(c->inbox), "_INBOX.");
	while (n < sizeof(c->inbox) - 1 && n < 7 + 22) {
		c->inbox[n++] = alphabet[sys_rand32_get() % (sizeof(alphabet) - 1)];
	}
	c->inbox[n] = '\0';

	snprintk(c->inbox_sub.subject, sizeof(c->inbox_sub.subject), "%s.*", c->inbox);
	c->inbox_sub.sid = INBOX_SID;

	return 0;
}

int nats_connect(struct nats_client *c, k_timeout_t timeout)
{
	k_timepoint_t end = sys_timepoint_calc(timeout);
	int ret;

	k_mutex_lock(&c->lock, K_FOREVER);
	if (c->running) {
		k_mutex_unlock(&c->lock);
		return -EALREADY;
	}
	c->running = true;
	c->stop = false;
	c->state = NATS_STATE_CONNECTING;
	k_mutex_unlock(&c->lock);

	k_thread_create(&c->thread, c->stack, K_KERNEL_STACK_SIZEOF(c->stack), client_thread, c,
			NULL, NULL, CONFIG_NATS_THREAD_PRIORITY, 0, K_NO_WAIT);
	k_thread_name_set(&c->thread, "nats");

	k_mutex_lock(&c->lock, K_FOREVER);
	while (c->state != NATS_STATE_CONNECTED && !sys_timepoint_expired(end)) {
		k_condvar_wait(&c->cond, &c->lock, sys_timepoint_timeout(end));
	}
	ret = c->state == NATS_STATE_CONNECTED ? 0 : -EAGAIN;
	k_mutex_unlock(&c->lock);

	return ret;
}

int nats_disconnect(struct nats_client *c)
{
	if (in_rx_thread(c)) {
		return -EDEADLK;
	}

	k_mutex_lock(&c->lock, K_FOREVER);
	if (!c->running) {
		k_mutex_unlock(&c->lock);
		return 0;
	}
	k_mutex_unlock(&c->lock);

	if (nats_is_connected(c)) {
		(void)nats_flush(c, K_SECONDS(1));
	}

	k_mutex_lock(&c->lock, K_FOREVER);
	c->stop = true;
	k_condvar_broadcast(&c->cond);
	k_mutex_unlock(&c->lock);

	/* Wake the receive thread from poll(). */
	k_mutex_lock(&c->tx_lock, K_FOREVER);
	if (c->sock >= 0) {
		zsock_shutdown(c->sock, ZSOCK_SHUT_RDWR);
	}
	k_mutex_unlock(&c->tx_lock);

	k_thread_join(&c->thread, K_FOREVER);

	k_mutex_lock(&c->lock, K_FOREVER);
	c->running = false;
	k_mutex_unlock(&c->lock);

	return 0;
}

bool nats_is_connected(struct nats_client *c)
{
	return c->state == NATS_STATE_CONNECTED;
}

static int publish(struct nats_client *c, const char *subject, const char *reply,
		   const struct nats_hdr *hdrs, size_t hdr_count, const void *data, size_t len)
{
	bool with_hdrs = hdrs != NULL;
	size_t reply_len = reply != NULL ? strlen(reply) : 0;
	size_t subject_len;
	size_t headroom;
	size_t line_len;
	int hdr_len = 0;
	int ret;

	if (!nats_subject_valid(subject, false) ||
	    (reply_len > 0 && !nats_subject_valid(reply, false)) || (data == NULL && len > 0)) {
		return -EINVAL;
	}

	if (with_hdrs && !IS_ENABLED(CONFIG_NATS_HEADERS)) {
		return -ENOTSUP;
	}

	subject_len = strlen(subject);
	/* Room for "HPUB <subject> <reply> <hdr len> <total len>\r\n" */
	headroom = subject_len + reply_len + 32;
	if (headroom >= sizeof(c->tx_buf)) {
		return -ENOMEM;
	}

	k_mutex_lock(&c->tx_lock, K_FOREVER);

	if (c->sock < 0 || c->state != NATS_STATE_CONNECTED) {
		ret = -ENOTCONN;
		goto out;
	}

	if (with_hdrs) {
		if (!c->server_headers) {
			ret = -ENOTSUP;
			goto out;
		}
		hdr_len = nats_hdr_encode((char *)c->tx_buf + headroom,
					  sizeof(c->tx_buf) - headroom, hdrs, hdr_count);
		if (hdr_len < 0) {
			ret = hdr_len;
			goto out;
		}
	}

	if (len + hdr_len > c->max_payload) {
		ret = -EMSGSIZE;
		goto out;
	}

	if (with_hdrs) {
		ret = snprintk((char *)c->tx_buf, headroom, "HPUB %s%s%s %d %zu\r\n", subject,
			       reply_len ? " " : "", reply_len ? reply : "", hdr_len,
			       hdr_len + len);
	} else {
		ret = snprintk((char *)c->tx_buf, headroom, "PUB %s%s%s %zu\r\n", subject,
			       reply_len ? " " : "", reply_len ? reply : "", len);
	}
	if (ret < 0 || ret >= headroom) {
		ret = -ENOMEM;
		goto out;
	}

	line_len = ret;
	if (hdr_len > 0) {
		memmove(c->tx_buf + line_len, c->tx_buf + headroom, hdr_len);
	}
	line_len += hdr_len;

	if (line_len + len + 2 <= sizeof(c->tx_buf)) {
		/* Send the whole message in one segment when it fits. */
		if (len > 0) {
			memcpy(c->tx_buf + line_len, data, len);
		}
		memcpy(c->tx_buf + line_len + len, "\r\n", 2);
		ret = tx_raw(c, c->tx_buf, line_len + len + 2);
	} else {
		ret = tx_raw(c, c->tx_buf, line_len);
		if (ret == 0) {
			ret = tx_raw(c, data, len);
		}
		if (ret == 0) {
			ret = tx_raw(c, "\r\n", 2);
		}
	}

	if (ret == 0) {
		c->stats.out_msgs++;
		c->stats.out_bytes += len;
	}

out:
	k_mutex_unlock(&c->tx_lock);
	return ret;
}

int nats_publish(struct nats_client *c, const char *subject, const void *data, size_t len)
{
	return publish(c, subject, NULL, NULL, 0, data, len);
}

int nats_publish_reply(struct nats_client *c, const char *subject, const char *reply,
		       const void *data, size_t len)
{
	return publish(c, subject, reply, NULL, 0, data, len);
}

int nats_publish_hdr(struct nats_client *c, const char *subject, const char *reply,
		     const struct nats_hdr *hdrs, size_t hdr_count, const void *data, size_t len)
{
	static const struct nats_hdr none[1];

	return publish(c, subject, reply, hdrs != NULL ? hdrs : none, hdrs != NULL ? hdr_count : 0,
		       data, len);
}

int nats_respond(struct nats_client *c, const struct nats_msg *msg, const void *data, size_t len)
{
	if (msg->reply_len == 0) {
		return -EINVAL;
	}

	return publish(c, msg->reply, NULL, NULL, 0, data, len);
}

int nats_subscribe(struct nats_client *c, struct nats_sub *sub, const char *subject,
		   const char *queue, nats_msg_cb_t cb, void *user_data)
{
	int slot = -ENOMEM;
	int ret = 0;

	if (sub == NULL || cb == NULL || !nats_subject_valid(subject, true) ||
	    strlen(subject) >= sizeof(sub->subject)) {
		return -EINVAL;
	}

	if (queue != NULL && (*queue == '\0' || strlen(queue) >= sizeof(sub->queue) ||
			      queue[strcspn(queue, " \t\r\n")] != '\0')) {
		return -EINVAL;
	}

	k_mutex_lock(&c->lock, K_FOREVER);

	if (find_sub(c, sub) >= 0) {
		ret = -EALREADY;
		goto out;
	}

	for (int i = 0; i < ARRAY_SIZE(c->subs); i++) {
		if (c->subs[i] == NULL) {
			slot = i;
			break;
		}
	}
	if (slot < 0) {
		ret = slot;
		goto out;
	}

	memset(sub, 0, sizeof(*sub));
	strcpy(sub->subject, subject);
	if (queue != NULL) {
		strcpy(sub->queue, queue);
	}
	sub->cb = cb;
	sub->user_data = user_data;
	sub->sid = c->next_sid++;
	c->subs[slot] = sub;

	if (c->subs_sent && send_sub(c, sub) < 0) {
		/* The subscription is sent again on reconnect. */
		LOG_WRN("SUB %s not sent", subject);
	}

out:
	k_mutex_unlock(&c->lock);
	return ret;
}

int nats_unsubscribe(struct nats_client *c, struct nats_sub *sub, uint32_t max_msgs)
{
	int slot;
	int ret = 0;

	k_mutex_lock(&c->lock, K_FOREVER);

	slot = find_sub(c, sub);
	if (slot < 0) {
		k_mutex_unlock(&c->lock);
		return -ENOENT;
	}

	if (max_msgs == 0 || max_msgs <= sub->delivered) {
		c->subs[slot] = NULL;
		max_msgs = 0;
	} else {
		sub->max_msgs = max_msgs;
	}

	if (c->subs_sent) {
		k_mutex_lock(&c->tx_lock, K_FOREVER);
		if (max_msgs == 0) {
			ret = tx_line(c, "UNSUB %u\r\n", sub->sid);
		} else {
			ret = tx_line(c, "UNSUB %u %u\r\n", sub->sid, max_msgs - sub->delivered);
		}
		k_mutex_unlock(&c->tx_lock);
	}

	k_mutex_unlock(&c->lock);

	/* Not reaching the server only matters while connected; a reconnect starts clean. */
	return ret == -ENOTCONN ? 0 : ret;
}

int nats_request(struct nats_client *c, const char *subject, const void *data, size_t len,
		 void *reply_buf, size_t reply_size, size_t *reply_len, k_timeout_t timeout)
{
	struct nats_pending *p = NULL;
	char reply[sizeof(c->inbox) + 12];
	int ret;

	if (in_rx_thread(c)) {
		return -EDEADLK;
	}

	if (reply_buf == NULL && reply_size > 0) {
		return -EINVAL;
	}

	k_mutex_lock(&c->lock, K_FOREVER);
	for (int i = 0; i < ARRAY_SIZE(c->pending); i++) {
		if (!c->pending[i].active) {
			p = &c->pending[i];
			break;
		}
	}
	if (p == NULL) {
		k_mutex_unlock(&c->lock);
		return -EBUSY;
	}
	p->active = true;
	p->token = ++c->next_token;
	p->buf = reply_buf;
	p->size = reply_size;
	p->len = 0;
	p->result = -ETIMEDOUT;
	k_sem_reset(&p->done);
	snprintk(reply, sizeof(reply), "%s.%u", c->inbox, p->token);
	k_mutex_unlock(&c->lock);

	ret = publish(c, subject, reply, NULL, 0, data, len);
	if (ret == 0) {
		(void)k_sem_take(&p->done, timeout);
	}

	k_mutex_lock(&c->lock, K_FOREVER);
	if (ret == 0) {
		ret = p->result;
		if (reply_len != NULL) {
			*reply_len = p->len;
		}
	}
	p->active = false;
	k_mutex_unlock(&c->lock);

	return ret;
}

int nats_flush(struct nats_client *c, k_timeout_t timeout)
{
	k_timepoint_t end = sys_timepoint_calc(timeout);
	uint32_t target;
	uint32_t gen;
	int ret;

	if (in_rx_thread(c)) {
		return -EDEADLK;
	}

	k_mutex_lock(&c->lock, K_FOREVER);
	if (c->state != NATS_STATE_CONNECTED) {
		k_mutex_unlock(&c->lock);
		return -ENOTCONN;
	}
	gen = c->conn_gen;
	k_mutex_unlock(&c->lock);

	ret = send_ping(c, &target);
	if (ret < 0) {
		return ret;
	}

	k_mutex_lock(&c->lock, K_FOREVER);
	for (;;) {
		if (c->conn_gen != gen) {
			ret = -ENOTCONN;
			break;
		}
		if ((int32_t)(c->pongs_recv - target) >= 0) {
			ret = 0;
			break;
		}
		if (sys_timepoint_expired(end)) {
			ret = -ETIMEDOUT;
			break;
		}
		k_condvar_wait(&c->cond, &c->lock, sys_timepoint_timeout(end));
	}
	k_mutex_unlock(&c->lock);

	return ret;
}

int nats_msg_header(const struct nats_msg *msg, const char *key, const char **val,
		    size_t *val_len)
{
	if (msg->hdr == NULL) {
		return -ENOENT;
	}

	return nats_hdr_get(msg->hdr, msg->hdr_len, key, val, val_len);
}

void nats_client_stats(struct nats_client *c, struct nats_stats *stats)
{
	k_mutex_lock(&c->lock, K_FOREVER);
	k_mutex_lock(&c->tx_lock, K_FOREVER);
	*stats = c->stats;
	k_mutex_unlock(&c->tx_lock);
	k_mutex_unlock(&c->lock);
}
