/*
 * Copyright (c) 2026 Cliff Brake
 * SPDX-License-Identifier: Apache-2.0
 *
 * Client tests against a real nats-server, run by scripts/run-client-tests.sh.
 *
 * The script passes server URLs in environment variables. Fault tests connect
 * through a TCP proxy in this file, which can drop or stall the connection.
 */

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <nsi_host_trampolines.h>

#include <zephyr/kernel.h>
#include <zephyr/net/nats.h>
#include <zephyr/net/socket.h>
#include <zephyr/ztest.h>

#include "nats_util.h"

static char url_open[64];
static char url_user[64];
static char url_token[64];
static char url_proxy[64];
static uint16_t proxy_port;
static uint16_t upstream_port;

/* Per-client event recorder */

struct events {
	struct k_sem connected;
	struct k_sem disconnected;
	struct k_sem reconnected;
	struct k_sem error;
	char err[96];
};

static void on_event(struct nats_client *c, enum nats_event event, const char *err, void *ud)
{
	struct events *ev = ud;

	ARG_UNUSED(c);

	switch (event) {
	case NATS_EVENT_CONNECTED:
		k_sem_give(&ev->connected);
		break;
	case NATS_EVENT_DISCONNECTED:
		k_sem_give(&ev->disconnected);
		break;
	case NATS_EVENT_RECONNECTED:
		k_sem_give(&ev->reconnected);
		break;
	case NATS_EVENT_ERROR:
		strncpy(ev->err, err, sizeof(ev->err) - 1);
		k_sem_give(&ev->error);
		break;
	}
}

static void events_init(struct events *ev)
{
	memset(ev, 0, sizeof(*ev));
	k_sem_init(&ev->connected, 0, 100);
	k_sem_init(&ev->disconnected, 0, 100);
	k_sem_init(&ev->reconnected, 0, 100);
	k_sem_init(&ev->error, 0, 100);
}

/* Per-subscription message recorder */

struct inbox {
	struct k_sem got;
	atomic_t count;
	char subject[64];
	char reply[64];
	char data[2048];
	size_t data_len;
	char hdr_val[32];
	bool has_hdr;
	const char *respond;
};

static void on_msg(struct nats_client *c, const struct nats_msg *msg, void *ud)
{
	struct inbox *in = ud;
	const char *v;
	size_t vlen;

	strncpy(in->subject, msg->subject, sizeof(in->subject) - 1);
	strncpy(in->reply, msg->reply, sizeof(in->reply) - 1);
	in->data_len = msg->data_len;
	memcpy(in->data, msg->data, MIN(msg->data_len + 1, sizeof(in->data)));
	in->has_hdr = nats_msg_header(msg, "X-Test", &v, &vlen) == 0;
	if (in->has_hdr) {
		memcpy(in->hdr_val, v, MIN(vlen, sizeof(in->hdr_val) - 1));
		in->hdr_val[MIN(vlen, sizeof(in->hdr_val) - 1)] = '\0';
	}

	if (in->respond != NULL && msg->reply_len > 0) {
		nats_respond(c, msg, in->respond, strlen(in->respond));
	}

	atomic_inc(&in->count);
	k_sem_give(&in->got);
}

static void inbox_init(struct inbox *in)
{
	memset(in, 0, sizeof(*in));
	k_sem_init(&in->got, 0, 1000);
}

/* Clients used by the tests; reset before each test. */

static struct nats_client client_a;
static struct nats_client client_b;
static struct events ev_a;
static struct events ev_b;

static void start(struct nats_client *c, struct events *ev, const char *url)
{
	struct nats_config cfg = {
		.url = url,
		.name = "ztest",
		.event_cb = on_event,
		.user_data = ev,
	};

	events_init(ev);
	zassert_ok(nats_client_init(c, &cfg));
	zassert_ok(nats_connect(c, K_SECONDS(5)), "cannot connect to %s", url);
}

/* TCP proxy for fault injection */

enum proxy_mode {
	PROXY_FORWARD,
	/* Keep the connection but drop everything the server sends */
	PROXY_BLACKHOLE,
	/* Accept and close at once, as a server that is down would */
	PROXY_REFUSE,
};

static atomic_t proxy_mode;
static atomic_t proxy_kill;
static atomic_t proxy_accepts;
static K_THREAD_STACK_DEFINE(proxy_stack, 4096);
static struct k_thread proxy_thread;

static int tcp_connect_local(uint16_t port)
{
	struct net_sockaddr_in sin = {
		.sin_family = NET_AF_INET,
		.sin_port = net_htons(port),
	};
	int s = zsock_socket(NET_AF_INET, NET_SOCK_STREAM, NET_IPPROTO_TCP);

	zsock_inet_pton(NET_AF_INET, "127.0.0.1", &sin.sin_addr);
	if (zsock_connect(s, (struct net_sockaddr *)&sin, sizeof(sin)) < 0) {
		zsock_close(s);
		return -1;
	}

	return s;
}

static void proxy_pipe(int down)
{
	int up = tcp_connect_local(upstream_port);
	struct zsock_pollfd fds[2] = {
		{.fd = down, .events = ZSOCK_POLLIN},
		{.fd = up, .events = ZSOCK_POLLIN},
	};
	uint8_t buf[512];

	if (up < 0) {
		return;
	}

	while (!atomic_cas(&proxy_kill, 1, 0)) {
		int ret = zsock_poll(fds, 2, 20);

		if (ret < 0) {
			break;
		}
		if (ret == 0) {
			continue;
		}

		if (fds[0].revents) {
			ssize_t n = zsock_recv(down, buf, sizeof(buf), 0);

			if (n <= 0 || zsock_send(up, buf, n, 0) < 0) {
				break;
			}
		}
		if (fds[1].revents) {
			ssize_t n = zsock_recv(up, buf, sizeof(buf), 0);

			if (n <= 0) {
				break;
			}
			if (atomic_get(&proxy_mode) != PROXY_BLACKHOLE &&
			    zsock_send(down, buf, n, 0) < 0) {
				break;
			}
		}
	}

	zsock_close(up);
}

static void proxy_main(void *p1, void *p2, void *p3)
{
	struct net_sockaddr_in sin = {
		.sin_family = NET_AF_INET,
		.sin_port = net_htons(proxy_port),
	};
	int one = 1;
	int ls;

	ARG_UNUSED(p1);
	ARG_UNUSED(p2);
	ARG_UNUSED(p3);

	ls = zsock_socket(NET_AF_INET, NET_SOCK_STREAM, NET_IPPROTO_TCP);
	zsock_setsockopt(ls, ZSOCK_SOL_SOCKET, ZSOCK_SO_REUSEADDR, &one, sizeof(one));
	zsock_inet_pton(NET_AF_INET, "127.0.0.1", &sin.sin_addr);
	__ASSERT(zsock_bind(ls, (struct net_sockaddr *)&sin, sizeof(sin)) == 0, "bind");
	__ASSERT(zsock_listen(ls, 2) == 0, "listen");

	for (;;) {
		int s = zsock_accept(ls, NULL, NULL);

		if (s < 0) {
			continue;
		}
		atomic_inc(&proxy_accepts);
		atomic_set(&proxy_kill, 0);
		if (atomic_get(&proxy_mode) != PROXY_REFUSE) {
			proxy_pipe(s);
		}
		zsock_close(s);
	}
}

/* Suite setup */

static const char *env(const char *name, const char *def)
{
	const char *v = nsi_host_getenv(name);

	return v != NULL && *v != '\0' ? v : def;
}

static void *suite_setup(void)
{
	struct nats_url u;

	strncpy(url_open, env("NATS_TEST_URL", "nats://127.0.0.1:4222"), sizeof(url_open) - 1);
	strncpy(url_user, env("NATS_TEST_USER_URL", ""), sizeof(url_user) - 1);
	strncpy(url_token, env("NATS_TEST_TOKEN_URL", ""), sizeof(url_token) - 1);
	proxy_port = atoi(env("NATS_TEST_PROXY_PORT", "14299"));
	snprintf(url_proxy, sizeof(url_proxy), "nats://127.0.0.1:%u", proxy_port);

	zassert_ok(nats_url_parse(url_open, &u));
	upstream_port = u.port;

	k_thread_create(&proxy_thread, proxy_stack, K_THREAD_STACK_SIZEOF(proxy_stack), proxy_main,
			NULL, NULL, NULL, 5, 0, K_NO_WAIT);
	k_thread_name_set(&proxy_thread, "proxy");

	return NULL;
}

static void before(void *f)
{
	ARG_UNUSED(f);
	atomic_set(&proxy_mode, PROXY_FORWARD);
	atomic_set(&proxy_kill, 0);
	atomic_set(&proxy_accepts, 0);
}

static void after(void *f)
{
	ARG_UNUSED(f);
	nats_disconnect(&client_a);
	nats_disconnect(&client_b);
}

ZTEST_SUITE(nats_client, NULL, suite_setup, before, after, NULL);

/* Tests */

ZTEST(nats_client, test_pubsub)
{
	static struct nats_sub sub;
	static struct inbox in;
	struct nats_stats st;

	inbox_init(&in);
	start(&client_a, &ev_a, url_open);
	zassert_ok(k_sem_take(&ev_a.connected, K_SECONDS(1)));
	zassert_true(nats_is_connected(&client_a));

	zassert_ok(nats_subscribe(&client_a, &sub, "t.pubsub.>", NULL, on_msg, &in));
	zassert_equal(nats_subscribe(&client_a, &sub, "t.x", NULL, on_msg, &in), -EALREADY);
	zassert_ok(nats_publish(&client_a, "t.pubsub.one", "hello", 5));
	zassert_ok(k_sem_take(&in.got, K_SECONDS(2)));
	zassert_str_equal(in.subject, "t.pubsub.one");
	zassert_str_equal(in.data, "hello");
	zassert_equal(in.data_len, 5);

	zassert_ok(nats_publish_reply(&client_a, "t.pubsub.two", "t.reply", "", 0));
	zassert_ok(k_sem_take(&in.got, K_SECONDS(2)));
	zassert_str_equal(in.reply, "t.reply");
	zassert_equal(in.data_len, 0);

	zassert_ok(nats_flush(&client_a, K_SECONDS(2)));
	nats_client_stats(&client_a, &st);
	zassert_equal(st.out_msgs, 2);
	zassert_equal(st.in_msgs, 2);
	zassert_equal(st.out_bytes, 5);
}

ZTEST(nats_client, test_invalid_args)
{
	static struct nats_sub sub;
	static struct inbox in;

	zassert_equal(nats_publish(&client_a, "t.x", "x", 1), -ENOTCONN);

	start(&client_a, &ev_a, url_open);
	zassert_equal(nats_publish(&client_a, "t.*", "x", 1), -EINVAL);
	zassert_equal(nats_publish(&client_a, "t x", "x", 1), -EINVAL);
	zassert_equal(nats_publish(&client_a, "", "x", 1), -EINVAL);
	zassert_equal(nats_subscribe(&client_a, &sub, "a..b", NULL, on_msg, &in), -EINVAL);
	zassert_equal(nats_subscribe(&client_a, &sub, "a", "bad queue", on_msg, &in), -EINVAL);
	zassert_equal(nats_unsubscribe(&client_a, &sub, 0), -ENOENT);
}

ZTEST(nats_client, test_echo_off)
{
	static struct nats_sub sub;
	static struct inbox in;
	struct nats_config cfg = {.url = url_open, .no_echo = true};

	inbox_init(&in);
	zassert_ok(nats_client_init(&client_a, &cfg));
	zassert_ok(nats_connect(&client_a, K_SECONDS(5)));
	start(&client_b, &ev_b, url_open);

	zassert_ok(nats_subscribe(&client_a, &sub, "t.echo", NULL, on_msg, &in));
	zassert_ok(nats_flush(&client_a, K_SECONDS(2)));
	zassert_ok(nats_publish(&client_a, "t.echo", "self", 4));
	zassert_ok(nats_publish(&client_b, "t.echo", "other", 5));
	zassert_ok(k_sem_take(&in.got, K_SECONDS(2)));
	zassert_str_equal(in.data, "other");
	zassert_equal(k_sem_take(&in.got, K_MSEC(300)), -EAGAIN);
}

ZTEST(nats_client, test_queue_group)
{
	static struct nats_sub sub_a;
	static struct nats_sub sub_b;
	static struct inbox in_a;
	static struct inbox in_b;

	inbox_init(&in_a);
	inbox_init(&in_b);
	start(&client_a, &ev_a, url_open);
	start(&client_b, &ev_b, url_open);

	zassert_ok(nats_subscribe(&client_a, &sub_a, "t.queue.*", "workers", on_msg, &in_a));
	zassert_ok(nats_subscribe(&client_b, &sub_b, "t.queue.*", "workers", on_msg, &in_b));
	zassert_ok(nats_flush(&client_a, K_SECONDS(2)));
	zassert_ok(nats_flush(&client_b, K_SECONDS(2)));

	for (int i = 0; i < 20; i++) {
		zassert_ok(nats_publish(&client_a, "t.queue.job", "j", 1));
	}
	zassert_ok(nats_flush(&client_a, K_SECONDS(2)));
	k_msleep(200);

	zassert_equal(atomic_get(&in_a.count) + atomic_get(&in_b.count), 20, "a %ld b %ld",
		      atomic_get(&in_a.count), atomic_get(&in_b.count));
}

ZTEST(nats_client, test_unsubscribe_after)
{
	static struct nats_sub sub;
	static struct inbox in;

	inbox_init(&in);
	start(&client_a, &ev_a, url_open);

	zassert_ok(nats_subscribe(&client_a, &sub, "t.unsub", NULL, on_msg, &in));
	zassert_ok(nats_unsubscribe(&client_a, &sub, 3));
	for (int i = 0; i < 6; i++) {
		zassert_ok(nats_publish(&client_a, "t.unsub", "m", 1));
	}
	zassert_ok(nats_flush(&client_a, K_SECONDS(2)));
	k_msleep(200);
	zassert_equal(atomic_get(&in.count), 3);

	/* The client removed the subscription after its last message. */
	zassert_equal(nats_unsubscribe(&client_a, &sub, 0), -ENOENT);

	zassert_ok(nats_subscribe(&client_a, &sub, "t.unsub", NULL, on_msg, &in));
	zassert_ok(nats_unsubscribe(&client_a, &sub, 0));
	zassert_ok(nats_publish(&client_a, "t.unsub", "m", 1));
	zassert_ok(nats_flush(&client_a, K_SECONDS(2)));
	k_msleep(100);
	zassert_equal(atomic_get(&in.count), 3);
}

ZTEST(nats_client, test_request_reply)
{
	static struct nats_sub svc;
	static struct nats_sub silent;
	static struct inbox in;
	static struct inbox in_silent;
	char reply[32];
	char tiny[4];
	size_t len = 0;

	inbox_init(&in);
	inbox_init(&in_silent);
	in.respond = "pong-reply";
	start(&client_a, &ev_a, url_open);
	start(&client_b, &ev_b, url_open);

	zassert_ok(nats_subscribe(&client_b, &svc, "t.svc", NULL, on_msg, &in));
	zassert_ok(nats_subscribe(&client_b, &silent, "t.silent", NULL, on_msg, &in_silent));
	zassert_ok(nats_flush(&client_b, K_SECONDS(2)));

	zassert_ok(nats_request(&client_a, "t.svc", "ping", 4, reply, sizeof(reply), &len,
				K_SECONDS(2)));
	zassert_equal(len, 10);
	zassert_str_equal(reply, "pong-reply");
	zassert_str_equal(in.data, "ping");

	zassert_equal(nats_request(&client_a, "t.svc", "ping", 4, tiny, sizeof(tiny), &len,
				   K_SECONDS(2)),
		      -ENOBUFS);
	zassert_equal(len, 10);
	zassert_mem_equal(tiny, "pong", 4);

	zassert_equal(nats_request(&client_a, "t.nobody", "x", 1, reply, sizeof(reply), &len,
				   K_SECONDS(2)),
		      -EHOSTUNREACH);

	zassert_equal(nats_request(&client_a, "t.silent", "x", 1, reply, sizeof(reply), &len,
				   K_MSEC(300)),
		      -ETIMEDOUT);
	zassert_ok(k_sem_take(&in_silent.got, K_SECONDS(1)));
}

ZTEST(nats_client, test_headers)
{
	static struct nats_sub sub;
	static struct inbox in;
	static const struct nats_hdr hdrs[] = {
		{"X-Test", "value-1"},
		{"Other", "2"},
	};

	inbox_init(&in);
	start(&client_a, &ev_a, url_open);

	zassert_ok(nats_subscribe(&client_a, &sub, "t.hdr", NULL, on_msg, &in));
	zassert_ok(nats_publish_hdr(&client_a, "t.hdr", NULL, hdrs, ARRAY_SIZE(hdrs), "body", 4));
	zassert_ok(k_sem_take(&in.got, K_SECONDS(2)));
	zassert_true(in.has_hdr);
	zassert_str_equal(in.hdr_val, "value-1");
	zassert_str_equal(in.data, "body");

	zassert_ok(nats_publish(&client_a, "t.hdr", "plain", 5));
	zassert_ok(k_sem_take(&in.got, K_SECONDS(2)));
	zassert_false(in.has_hdr);
}

ZTEST(nats_client, test_large_and_dropped)
{
	static struct nats_sub sub;
	static struct inbox in;
	static char big[2000];
	struct nats_stats st;

	inbox_init(&in);
	start(&client_a, &ev_a, url_open);
	zassert_ok(nats_subscribe(&client_a, &sub, "t.big", NULL, on_msg, &in));

	/* Larger than the transmit buffer, but fits in the receive buffer. */
	for (int i = 0; i < 800; i++) {
		big[i] = 'a' + i % 26;
	}
	zassert_ok(nats_publish(&client_a, "t.big", big, 800));
	zassert_ok(k_sem_take(&in.got, K_SECONDS(2)));
	zassert_equal(in.data_len, 800);
	zassert_mem_equal(in.data, big, 800);

	/* Larger than the receive buffer: dropped, and the connection stays up. */
	zassert_ok(nats_publish(&client_a, "t.big", big, sizeof(big)));
	zassert_ok(nats_publish(&client_a, "t.big", "after", 5));
	zassert_ok(k_sem_take(&in.got, K_SECONDS(2)));
	zassert_str_equal(in.data, "after");

	nats_client_stats(&client_a, &st);
	zassert_equal(st.dropped, 1);
	zassert_true(nats_is_connected(&client_a));
}

static void check_auth(const char *url, const char *user, const char *pass, const char *token,
		       bool ok)
{
	struct nats_config cfg = {
		.url = url,
		.user = user,
		.pass = pass,
		.token = token,
		.event_cb = on_event,
		.user_data = &ev_a,
	};
	int ret;

	events_init(&ev_a);
	zassert_ok(nats_client_init(&client_a, &cfg));
	ret = nats_connect(&client_a, K_SECONDS(2));

	if (ok) {
		zassert_ok(ret);
	} else {
		zassert_equal(ret, -EAGAIN);
		zassert_ok(k_sem_take(&ev_a.error, K_SECONDS(1)));
		zassert_not_null(strstr(ev_a.err, "Authorization"), "err: %s", ev_a.err);
	}

	nats_disconnect(&client_a);
}

ZTEST(nats_client, test_auth_user)
{
	char url[96];
	struct nats_url u;

	if (url_user[0] == '\0') {
		ztest_test_skip();
	}

	check_auth(url_user, "test", "secret", NULL, true);
	check_auth(url_user, "test", "wrong", NULL, false);
	check_auth(url_user, NULL, NULL, NULL, false);

	zassert_ok(nats_url_parse(url_user, &u));
	snprintf(url, sizeof(url), "nats://test:secret@%s:%u", u.host, u.port);
	check_auth(url, NULL, NULL, NULL, true);
}

ZTEST(nats_client, test_auth_token)
{
	char url[96];
	struct nats_url u;

	if (url_token[0] == '\0') {
		ztest_test_skip();
	}

	check_auth(url_token, NULL, NULL, "s3cret", true);
	check_auth(url_token, NULL, NULL, "nope", false);

	zassert_ok(nats_url_parse(url_token, &u));
	snprintf(url, sizeof(url), "nats://s3cret@%s:%u", u.host, u.port);
	check_auth(url, NULL, NULL, NULL, true);
}

ZTEST(nats_client, test_reconnect)
{
	static struct nats_sub sub;
	static struct inbox in;
	struct nats_stats st;

	inbox_init(&in);
	start(&client_a, &ev_a, url_proxy);
	start(&client_b, &ev_b, url_open);
	zassert_ok(nats_subscribe(&client_a, &sub, "t.reconnect", NULL, on_msg, &in));
	zassert_ok(nats_flush(&client_a, K_SECONDS(2)));

	atomic_set(&proxy_kill, 1);
	zassert_ok(k_sem_take(&ev_a.disconnected, K_SECONDS(2)));
	zassert_equal(nats_publish(&client_a, "t.reconnect", "x", 1), -ENOTCONN);
	zassert_ok(k_sem_take(&ev_a.reconnected, K_SECONDS(3)));
	zassert_true(nats_is_connected(&client_a));

	/* The subscription was restored on the new connection. */
	zassert_ok(nats_publish(&client_b, "t.reconnect", "back", 4));
	zassert_ok(k_sem_take(&in.got, K_SECONDS(2)));
	zassert_str_equal(in.data, "back");

	nats_client_stats(&client_a, &st);
	zassert_equal(st.reconnects, 1);
	zassert_equal(k_sem_count_get(&ev_a.connected), 1);
}

ZTEST(nats_client, test_stale_connection)
{
	start(&client_a, &ev_a, url_proxy);

	/* The server stops answering, so PINGs go unanswered. */
	atomic_set(&proxy_mode, PROXY_BLACKHOLE);
	zassert_ok(k_sem_take(&ev_a.disconnected,
			      K_MSEC(CONFIG_NATS_PING_INTERVAL_MS * (CONFIG_NATS_MAX_PINGS_OUT + 2))));

	atomic_set(&proxy_mode, PROXY_FORWARD);
	atomic_set(&proxy_kill, 1);
	zassert_ok(k_sem_take(&ev_a.reconnected, K_SECONDS(5)));
}

ZTEST(nats_client, test_backoff)
{
	struct nats_config cfg = {.url = url_proxy, .event_cb = on_event, .user_data = &ev_a};
	int64_t t0;
	long accepts;

	atomic_set(&proxy_mode, PROXY_REFUSE);
	events_init(&ev_a);
	zassert_ok(nats_client_init(&client_a, &cfg));
	t0 = k_uptime_get();
	zassert_equal(nats_connect(&client_a, K_MSEC(2000)), -EAGAIN);
	accepts = atomic_get(&proxy_accepts);

	/*
	 * With delays starting at RECONNECT_MIN_MS and doubling to RECONNECT_MAX_MS,
	 * two seconds allows a handful of attempts: more than one, but no busy loop.
	 */
	zassert_true(accepts >= 3 && accepts <= 2000 / (CONFIG_NATS_RECONNECT_MIN_MS / 2) + 1,
		     "%ld attempts in %lld ms", accepts, k_uptime_get() - t0);

	/* The server comes back, and the client connects without being asked again. */
	atomic_set(&proxy_mode, PROXY_FORWARD);
	zassert_ok(k_sem_take(&ev_a.connected, K_MSEC(CONFIG_NATS_RECONNECT_MAX_MS * 2)));
}

ZTEST(nats_client, test_disconnect_and_restart)
{
	static struct nats_sub sub;
	static struct inbox in;

	inbox_init(&in);
	start(&client_a, &ev_a, url_open);
	zassert_ok(nats_subscribe(&client_a, &sub, "t.restart", NULL, on_msg, &in));

	zassert_ok(nats_disconnect(&client_a));
	zassert_ok(k_sem_take(&ev_a.disconnected, K_SECONDS(1)));
	zassert_false(nats_is_connected(&client_a));
	zassert_equal(nats_publish(&client_a, "t.restart", "x", 1), -ENOTCONN);
	zassert_equal(nats_flush(&client_a, K_SECONDS(1)), -ENOTCONN);

	/* Connecting again restores the subscription. */
	zassert_ok(nats_connect(&client_a, K_SECONDS(5)));
	zassert_equal(nats_connect(&client_a, K_SECONDS(5)), -EALREADY);
	zassert_ok(k_sem_take(&ev_a.reconnected, K_SECONDS(1)));
	zassert_ok(nats_publish(&client_a, "t.restart", "again", 5));
	zassert_ok(k_sem_take(&in.got, K_SECONDS(2)));
	zassert_str_equal(in.data, "again");
}

#define BURST_THREADS 3
#define BURST_MSGS    50

static K_THREAD_STACK_ARRAY_DEFINE(burst_stacks, BURST_THREADS, 2048);
static struct k_thread burst_threads[BURST_THREADS];

static void burst(void *p1, void *p2, void *p3)
{
	char msg[16];

	ARG_UNUSED(p2);
	ARG_UNUSED(p3);

	for (int i = 0; i < BURST_MSGS; i++) {
		int n = snprintf(msg, sizeof(msg), "%d-%d", (int)(intptr_t)p1, i);

		zassert_ok(nats_publish(&client_a, "t.concurrent", msg, n));
	}
}

ZTEST(nats_client, test_concurrent_publish)
{
	static struct nats_sub sub;
	static struct inbox in;

	inbox_init(&in);
	start(&client_a, &ev_a, url_open);
	start(&client_b, &ev_b, url_open);
	zassert_ok(nats_subscribe(&client_b, &sub, "t.concurrent", NULL, on_msg, &in));
	zassert_ok(nats_flush(&client_b, K_SECONDS(2)));

	for (int i = 0; i < BURST_THREADS; i++) {
		k_thread_create(&burst_threads[i], burst_stacks[i],
				K_THREAD_STACK_SIZEOF(burst_stacks[i]), burst, (void *)(intptr_t)i,
				NULL, NULL, 6, 0, K_NO_WAIT);
	}
	for (int i = 0; i < BURST_THREADS; i++) {
		k_thread_join(&burst_threads[i], K_FOREVER);
	}

	zassert_ok(nats_flush(&client_a, K_SECONDS(2)));
	for (int i = 0; i < 20 && atomic_get(&in.count) < BURST_THREADS * BURST_MSGS; i++) {
		k_msleep(50);
	}
	zassert_equal(atomic_get(&in.count), BURST_THREADS * BURST_MSGS);
}

#ifdef CONFIG_NATS_TLS

#define TEST_SEC_TAG 1

ZTEST(nats_client, test_tls)
{
#ifdef NATS_TEST_HAVE_CA
	static const unsigned char ca[] = {
#include "nats_test_ca.inc"
	};
	static const sec_tag_t tags[] = {TEST_SEC_TAG};
	static bool ca_added;
	static struct nats_sub sub;
	static struct inbox in;
	const char *url = env("NATS_TEST_TLS_URL", "");
	struct nats_config cfg = {
		.url = url,
		.sec_tags = tags,
		.sec_tag_count = ARRAY_SIZE(tags),
		.tls_hostname = "localhost",
		.event_cb = on_event,
		.user_data = &ev_a,
	};
	char reply[16];
	size_t len;

	if (*url == '\0') {
		ztest_test_skip();
	}

	if (!ca_added) {
		zassert_ok(tls_credential_add(TEST_SEC_TAG, TLS_CREDENTIAL_CA_CERTIFICATE, ca,
					      sizeof(ca)));
		ca_added = true;
	}

	/* A name the certificate does not cover is rejected. */
	cfg.tls_hostname = "wrong.example";
	events_init(&ev_a);
	zassert_ok(nats_client_init(&client_a, &cfg));
	zassert_equal(nats_connect(&client_a, K_SECONDS(2)), -EAGAIN);
	nats_disconnect(&client_a);

	cfg.tls_hostname = "localhost";
	inbox_init(&in);
	in.respond = "secure";
	events_init(&ev_a);
	zassert_ok(nats_client_init(&client_a, &cfg));
	zassert_ok(nats_connect(&client_a, K_SECONDS(5)));

	zassert_ok(nats_subscribe(&client_a, &sub, "t.tls", NULL, on_msg, &in));
	zassert_ok(nats_request(&client_a, "t.tls", "hi", 2, reply, sizeof(reply), &len,
				K_SECONDS(2)));
	zassert_str_equal(reply, "secure");
	zassert_str_equal(in.data, "hi");
#else
	ztest_test_skip();
#endif
}

#endif /* CONFIG_NATS_TLS */
