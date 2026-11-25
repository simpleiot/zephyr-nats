/*
 * Copyright (c) 2026 Cliff Brake
 * SPDX-License-Identifier: Apache-2.0
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>

#include <zephyr/ztest.h>

#include "nats_parser.h"
#include "nats_util.h"

/* A copy of a parsed operation, since parser results do not outlive the next call. */
struct rec {
	enum nats_op_type type;
	char subject[32];
	char reply[32];
	char args[96];
	uint32_t sid;
	char hdr[64];
	size_t hdr_len;
	char data[64];
	size_t data_len;
};

#define MAX_RECS 16

static struct rec recs[MAX_RECS];
static int nrecs;
static int last_err;

static void record(const struct nats_op *op)
{
	struct rec *r;

	zassert_true(nrecs < MAX_RECS);
	r = &recs[nrecs++];
	memset(r, 0, sizeof(*r));
	r->type = op->type;
	r->sid = op->sid;
	if (op->subject != NULL) {
		zassert_equal(strlen(op->subject), op->subject_len);
		strncpy(r->subject, op->subject, sizeof(r->subject) - 1);
	}
	if (op->reply != NULL) {
		zassert_equal(strlen(op->reply), op->reply_len);
		strncpy(r->reply, op->reply, sizeof(r->reply) - 1);
	}
	if (op->args != NULL) {
		zassert_equal(strlen(op->args), op->args_len);
		strncpy(r->args, op->args, sizeof(r->args) - 1);
	}
	if (op->hdr != NULL) {
		zassert_true(op->hdr_len < sizeof(r->hdr));
		memcpy(r->hdr, op->hdr, op->hdr_len);
		r->hdr_len = op->hdr_len;
	}
	r->data_len = op->data_len;
	if (op->data != NULL) {
		zassert_true(op->data_len < sizeof(r->data));
		zassert_equal(op->data[op->data_len], '\0', "payload not NUL-terminated");
		memcpy(r->data, op->data, op->data_len);
	}
}

/* Feed bytes in chunks of at most chunk bytes, recording every operation. */
static void feed(struct nats_parser *p, const char *data, size_t len, size_t chunk)
{
	struct nats_op op;
	int ret;

	while (len > 0) {
		uint8_t *space;
		size_t avail = nats_parser_space(p, &space);
		size_t n = MIN(MIN(len, chunk), avail);

		zassert_true(avail > 0, "parser left no space");
		memcpy(space, data, n);
		nats_parser_commit(p, n);
		data += n;
		len -= n;

		while ((ret = nats_parser_next(p, &op)) > 0) {
			record(&op);
		}
		if (ret < 0) {
			last_err = ret;
			return;
		}
	}
}

static void reset_recs(void)
{
	memset(recs, 0, sizeof(recs));
	nrecs = 0;
	last_err = 0;
}

static uint8_t buf[256];
static struct nats_parser parser;

static void before(void *f)
{
	ARG_UNUSED(f);
	reset_recs();
	nats_parser_init(&parser, buf, sizeof(buf));
}

ZTEST_SUITE(nats_parser, NULL, NULL, before, NULL, NULL);

static const char stream[] = "INFO {\"server_id\":\"x\",\"max_payload\":1048576}\r\n"
			     "PING\r\n"
			     "MSG foo.bar 9 5\r\nhello\r\n"
			     "MSG foo.baz 10 _INBOX.abc.1 0\r\n\r\n"
			     "HMSG a.b 11 19 24\r\nNATS/1.0\r\nK: v1\r\n\r\nworld\r\n"
			     "HMSG a.c 12 rep 16 16\r\nNATS/1.0 503\r\n\r\n\r\n"
			     "PONG\r\n"
			     "+OK\r\n"
			     "-ERR 'Authorization Violation'\r\n";

static void check_stream(void)
{
	zassert_equal(last_err, 0, "error %d", last_err);
	zassert_equal(nrecs, 9, "got %d ops", nrecs);

	zassert_equal(recs[0].type, NATS_OP_INFO);
	zassert_str_equal(recs[0].args, "{\"server_id\":\"x\",\"max_payload\":1048576}");

	zassert_equal(recs[1].type, NATS_OP_PING);

	zassert_equal(recs[2].type, NATS_OP_MSG);
	zassert_str_equal(recs[2].subject, "foo.bar");
	zassert_str_equal(recs[2].reply, "");
	zassert_equal(recs[2].sid, 9);
	zassert_equal(recs[2].data_len, 5);
	zassert_str_equal(recs[2].data, "hello");

	zassert_equal(recs[3].type, NATS_OP_MSG);
	zassert_str_equal(recs[3].reply, "_INBOX.abc.1");
	zassert_equal(recs[3].data_len, 0);

	zassert_equal(recs[4].type, NATS_OP_HMSG);
	zassert_str_equal(recs[4].subject, "a.b");
	zassert_equal(recs[4].hdr_len, 19);
	zassert_mem_equal(recs[4].hdr, "NATS/1.0\r\nK: v1\r\n\r\n", 19);
	zassert_str_equal(recs[4].data, "world");

	zassert_equal(recs[5].type, NATS_OP_HMSG);
	zassert_str_equal(recs[5].reply, "rep");
	zassert_equal(recs[5].data_len, 0);
	zassert_equal(nats_hdr_status(recs[5].hdr, recs[5].hdr_len), 503);

	zassert_equal(recs[6].type, NATS_OP_PONG);
	zassert_equal(recs[7].type, NATS_OP_OK);
	zassert_equal(recs[8].type, NATS_OP_ERR);
	zassert_str_equal(recs[8].args, "Authorization Violation");
}

ZTEST(nats_parser, test_stream_whole)
{
	feed(&parser, stream, strlen(stream), SIZE_MAX);
	check_stream();
}

ZTEST(nats_parser, test_stream_byte_at_a_time)
{
	feed(&parser, stream, strlen(stream), 1);
	check_stream();
}

ZTEST(nats_parser, test_stream_every_split)
{
	size_t len = strlen(stream);

	for (size_t split = 0; split <= len; split++) {
		reset_recs();
		nats_parser_init(&parser, buf, sizeof(buf));
		feed(&parser, stream, split, SIZE_MAX);
		feed(&parser, stream + split, len - split, SIZE_MAX);
		check_stream();
	}
}

ZTEST(nats_parser, test_case_and_blank_lines)
{
	static const char s[] = "\r\nping\r\n\npong\nmsg x 1 2\r\nhi\r\n";

	feed(&parser, s, strlen(s), SIZE_MAX);
	zassert_equal(last_err, 0);
	zassert_equal(nrecs, 3);
	zassert_equal(recs[0].type, NATS_OP_PING);
	zassert_equal(recs[1].type, NATS_OP_PONG);
	zassert_equal(recs[2].type, NATS_OP_MSG);
	zassert_str_equal(recs[2].data, "hi");
}

ZTEST(nats_parser, test_oversize_dropped)
{
	static uint8_t small[64];
	char s[400];
	char payload[201];
	size_t len;

	memset(payload, 'x', 200);
	payload[200] = '\0';
	len = snprintf(s, sizeof(s), "MSG big 3 200\r\n%s\r\nPING\r\nMSG ok 4 2\r\nhi\r\n",
		       payload);

	for (size_t chunk = 1; chunk <= 64; chunk *= 2) {
		reset_recs();
		nats_parser_init(&parser, small, sizeof(small));
		feed(&parser, s, len, chunk);

		zassert_equal(last_err, 0, "chunk %zu: error %d", chunk, last_err);
		zassert_equal(nrecs, 3, "chunk %zu: %d ops", chunk, nrecs);
		zassert_equal(recs[0].type, NATS_OP_DROPPED);
		zassert_str_equal(recs[0].subject, "big");
		zassert_equal(recs[0].data_len, 200);
		zassert_equal(recs[1].type, NATS_OP_PING);
		zassert_equal(recs[2].type, NATS_OP_MSG);
		zassert_str_equal(recs[2].data, "hi");
		zassert_equal(parser.dropped, 1);
	}
}

ZTEST(nats_parser, test_exact_fit)
{
	static uint8_t small[32];
	/* 13-byte line + 17-byte payload + CRLF fills the buffer exactly. */
	static const char s[] = "MSG a 1 17\r\n12345678901234567\r\n";

	zassert_equal(strlen(s), 31);
	nats_parser_init(&parser, small, 31);
	feed(&parser, s, strlen(s), 1);
	zassert_equal(last_err, 0);
	zassert_equal(nrecs, 1);
	zassert_equal(recs[0].type, NATS_OP_MSG);
	zassert_str_equal(recs[0].data, "12345678901234567");
}

ZTEST(nats_parser, test_line_too_long)
{
	static uint8_t small[32];
	char s[40];

	memset(s, 'A', sizeof(s));
	nats_parser_init(&parser, small, sizeof(small));
	feed(&parser, s, sizeof(s), SIZE_MAX);
	zassert_equal(last_err, -EMSGSIZE);
}

ZTEST(nats_parser, test_bad_messages)
{
	static const char *const bad[] = {
		"FOO\r\n",
		"MSG a 1 xx\r\nhi\r\n",
		"MSG a 1\r\n",
		"MSG a b c d e f\r\n",
		"MSG a 1 3\r\nabcXY",
		"HMSG a 1 5 3\r\nabc\r\n",
		"MSG a x 2\r\nhi\r\n",
	};

	for (size_t i = 0; i < ARRAY_SIZE(bad); i++) {
		reset_recs();
		nats_parser_init(&parser, buf, sizeof(buf));
		feed(&parser, bad[i], strlen(bad[i]), SIZE_MAX);
		zassert_equal(last_err, -EBADMSG, "case %zu: %d", i, last_err);
	}
}

ZTEST_SUITE(nats_util, NULL, NULL, NULL, NULL, NULL);

ZTEST(nats_util, test_url)
{
	struct nats_url u;

	zassert_ok(nats_url_parse("nats://127.0.0.1:4333", &u));
	zassert_str_equal(u.host, "127.0.0.1");
	zassert_equal(u.port, 4333);
	zassert_false(u.tls);
	zassert_is_null(u.user);
	zassert_is_null(u.token);

	zassert_ok(nats_url_parse("demo.nats.io", &u));
	zassert_str_equal(u.host, "demo.nats.io");
	zassert_equal(u.port, 4222);

	zassert_ok(nats_url_parse("tls://bob:p@ss@host/", &u));
	zassert_true(u.tls);
	zassert_str_equal(u.host, "host");
	zassert_equal(u.user_len, 3);
	zassert_mem_equal(u.user, "bob", 3);
	zassert_equal(u.pass_len, 4);
	zassert_mem_equal(u.pass, "p@ss", 4);

	zassert_ok(nats_url_parse("nats://s3cret@[::1]:5000", &u));
	zassert_str_equal(u.host, "::1");
	zassert_equal(u.port, 5000);
	zassert_equal(u.token_len, 6);
	zassert_mem_equal(u.token, "s3cret", 6);

	zassert_equal(nats_url_parse("http://host", &u), -EINVAL);
	zassert_equal(nats_url_parse("nats://", &u), -EINVAL);
	zassert_equal(nats_url_parse("nats://host:0", &u), -EINVAL);
	zassert_equal(nats_url_parse("nats://host:70000", &u), -EINVAL);
	zassert_equal(nats_url_parse("nats://host:12ab", &u), -EINVAL);
	zassert_equal(nats_url_parse("nats://[::1", &u), -EINVAL);
	zassert_equal(nats_url_parse("nats://host/path", &u), -EINVAL);
	zassert_equal(nats_url_parse(NULL, &u), -EINVAL);
}

ZTEST(nats_util, test_subject)
{
	zassert_true(nats_subject_valid("a", false));
	zassert_true(nats_subject_valid("a.b-c.d_e", false));
	zassert_true(nats_subject_valid("a.*.c", true));
	zassert_true(nats_subject_valid("a.>", true));
	zassert_true(nats_subject_valid(">", true));

	zassert_false(nats_subject_valid("", true));
	zassert_false(nats_subject_valid(NULL, true));
	zassert_false(nats_subject_valid("a..b", true));
	zassert_false(nats_subject_valid(".a", true));
	zassert_false(nats_subject_valid("a.", true));
	zassert_false(nats_subject_valid("a b", true));
	zassert_false(nats_subject_valid("a\tb", true));
	zassert_false(nats_subject_valid("a\r\n", true));
	zassert_false(nats_subject_valid("a.*", false));
	zassert_false(nats_subject_valid("a.>", false));
	zassert_false(nats_subject_valid("a.>.b", true));
}

ZTEST(nats_util, test_json)
{
	static const char info[] =
		"{\"server_id\":\"NAB\",\"version\":\"2.10.0\",\"headers\":true,"
		"\"nested\":{\"max_payload\":5},\"connect_urls\":[\"a:1\",\"b,c:2\"],"
		"\"name\":\"x\\\"max_payload\\\":7\", \"max_payload\" : 1048576,"
		"\"tls_required\":false}";
	size_t len = strlen(info);
	const char *v;
	size_t vlen;
	uint32_t n;
	bool b;

	zassert_ok(nats_json_get_uint(info, len, "max_payload", &n));
	zassert_equal(n, 1048576);
	zassert_ok(nats_json_get_bool(info, len, "headers", &b));
	zassert_true(b);
	zassert_ok(nats_json_get_bool(info, len, "tls_required", &b));
	zassert_false(b);
	zassert_ok(nats_json_get(info, len, "connect_urls", &v, &vlen));
	zassert_equal(vlen, strlen("[\"a:1\",\"b,c:2\"]"));
	zassert_ok(nats_json_get(info, len, "server_id", &v, &vlen));
	zassert_mem_equal(v, "\"NAB\"", vlen);
	zassert_equal(nats_json_get(info, len, "missing", &v, &vlen), -ENOENT);
	zassert_equal(nats_json_get_uint(info, len, "version", &n), -EINVAL);
}

ZTEST(nats_util, test_json_escape)
{
	char out[32];
	int n;

	n = nats_json_escape(out, sizeof(out), "a\"b\\c\n", 6);
	zassert_equal(n, 13);
	zassert_mem_equal(out, "a\\\"b\\\\c\\u000a", 13);
	zassert_equal(nats_json_escape(out, 3, "a\"b", 3), -ENOMEM);
}

ZTEST(nats_util, test_headers)
{
	static const struct nats_hdr hdrs[] = {
		{"Nats-Msg-Id", "42"},
		{"X", ""},
	};
	static const struct nats_hdr bad[] = {{"a:b", "v"}};
	static const struct nats_hdr bad2[] = {{"a", "v\r\n"}};
	char out[64];
	const char *v;
	size_t vlen;
	int n;

	n = nats_hdr_encode(out, sizeof(out), hdrs, ARRAY_SIZE(hdrs));
	zassert_equal(n, strlen("NATS/1.0\r\nNats-Msg-Id: 42\r\nX: \r\n\r\n"));
	zassert_mem_equal(out, "NATS/1.0\r\nNats-Msg-Id: 42\r\nX: \r\n\r\n", n);

	zassert_ok(nats_hdr_get((uint8_t *)out, n, "Nats-Msg-Id", &v, &vlen));
	zassert_equal(vlen, 2);
	zassert_mem_equal(v, "42", 2);
	zassert_ok(nats_hdr_get((uint8_t *)out, n, "X", &v, &vlen));
	zassert_equal(vlen, 0);
	zassert_equal(nats_hdr_get((uint8_t *)out, n, "nats-msg-id", &v, &vlen), -ENOENT);
	zassert_equal(nats_hdr_get((uint8_t *)out, n, "Nats", &v, &vlen), -ENOENT);
	zassert_equal(nats_hdr_status((uint8_t *)out, n), 0);

	zassert_equal(nats_hdr_encode(out, 12, hdrs, ARRAY_SIZE(hdrs)), -ENOMEM);
	zassert_equal(nats_hdr_encode(out, sizeof(out), bad, 1), -EINVAL);
	zassert_equal(nats_hdr_encode(out, sizeof(out), bad2, 1), -EINVAL);

	zassert_equal(nats_hdr_status((const uint8_t *)"NATS/1.0 503\r\n\r\n", 16), 503);
	zassert_equal(nats_hdr_status((const uint8_t *)"NATS/1.0 408 Request Timeout\r\n\r\n",
				      32),
		      408);
}
