/*
 * Copyright (c) 2026 Cliff Brake
 * SPDX-License-Identifier: Apache-2.0
 */

#include <errno.h>
#include <stdbool.h>
#include <string.h>

#include <zephyr/sys/util.h>

#include "nats_parser.h"

#define MAX_TOKENS 5

struct token {
	size_t start;
	size_t len;
};

void nats_parser_init(struct nats_parser *p, uint8_t *buf, size_t size)
{
	p->buf = buf;
	p->size = size;
	p->dropped = 0;
	nats_parser_reset(p);
}

void nats_parser_reset(struct nats_parser *p)
{
	p->len = 0;
	p->consumed = 0;
	p->skip = 0;
}

static void discard(struct nats_parser *p, size_t n)
{
	if (n >= p->len) {
		p->len = 0;
		return;
	}

	memmove(p->buf, p->buf + n, p->len - n);
	p->len -= n;
}

/* Apply the bytes consumed by the previous operation and any pending skip. */
static void compact(struct nats_parser *p)
{
	size_t n;

	if (p->consumed > 0) {
		discard(p, p->consumed);
		p->consumed = 0;
	}

	if (p->skip > 0) {
		n = MIN(p->skip, p->len);
		discard(p, n);
		p->skip -= n;
	}
}

size_t nats_parser_space(struct nats_parser *p, uint8_t **ptr)
{
	compact(p);
	*ptr = p->buf + p->len;
	return p->size - p->len;
}

void nats_parser_commit(struct nats_parser *p, size_t n)
{
	p->len += MIN(n, p->size - p->len);
}

static bool is_space(uint8_t c)
{
	return c == ' ' || c == '\t';
}

/* Split buf[start, end) into whitespace-separated tokens without modifying it. */
static int tokenize(const uint8_t *buf, size_t start, size_t end, struct token *tok)
{
	int n = 0;
	size_t i = start;

	while (i < end) {
		while (i < end && is_space(buf[i])) {
			i++;
		}
		if (i == end) {
			break;
		}
		if (n == MAX_TOKENS) {
			return -EBADMSG;
		}
		tok[n].start = i;
		while (i < end && !is_space(buf[i])) {
			i++;
		}
		tok[n].len = i - tok[n].start;
		n++;
	}

	return n;
}

static int parse_size(const uint8_t *s, size_t len, size_t *out)
{
	size_t v = 0;

	if (len == 0 || len > 9) {
		return -EBADMSG;
	}

	for (size_t i = 0; i < len; i++) {
		if (s[i] < '0' || s[i] > '9') {
			return -EBADMSG;
		}
		v = v * 10 + (s[i] - '0');
	}

	*out = v;
	return 0;
}

static bool op_is(const uint8_t *s, size_t len, const char *name)
{
	size_t n = strlen(name);

	if (len != n) {
		return false;
	}

	for (size_t i = 0; i < n; i++) {
		uint8_t c = s[i];

		if (c >= 'a' && c <= 'z') {
			c -= 'a' - 'A';
		}
		if (c != (uint8_t)name[i]) {
			return false;
		}
	}

	return true;
}

static char *terminate(struct nats_parser *p, const struct token *t)
{
	p->buf[t->start + t->len] = '\0';
	return (char *)p->buf + t->start;
}

static int parse_msg(struct nats_parser *p, struct nats_op *op, bool hmsg, size_t args,
		     size_t line_end, size_t next)
{
	struct token tok[MAX_TOKENS];
	size_t hdr_len = 0;
	size_t total_len;
	size_t sid;
	size_t frame;
	int ntok;
	int want = hmsg ? 4 : 3;

	ntok = tokenize(p->buf, args, line_end, tok);
	if (ntok != want && ntok != want + 1) {
		return -EBADMSG;
	}

	if (parse_size(p->buf + tok[ntok - 1].start, tok[ntok - 1].len, &total_len) < 0) {
		return -EBADMSG;
	}

	if (hmsg) {
		if (parse_size(p->buf + tok[ntok - 2].start, tok[ntok - 2].len, &hdr_len) < 0 ||
		    hdr_len > total_len) {
			return -EBADMSG;
		}
	}

	if (parse_size(p->buf + tok[1].start, tok[1].len, &sid) < 0) {
		return -EBADMSG;
	}

	frame = next + total_len + 2;

	if (frame > p->size) {
		/* Too large to ever fit: report it and skip the payload. */
		*op = (struct nats_op){
			.type = NATS_OP_DROPPED,
			.sid = sid,
			.data_len = total_len - hdr_len,
		};
		op->subject = terminate(p, &tok[0]);
		op->subject_len = tok[0].len;
		p->consumed = MIN(frame, p->len);
		p->skip = frame - p->consumed;
		p->dropped++;
		return 1;
	}

	if (p->len < frame) {
		return 0;
	}

	if (p->buf[frame - 2] != '\r' || p->buf[frame - 1] != '\n') {
		return -EBADMSG;
	}

	*op = (struct nats_op){
		.type = hmsg ? NATS_OP_HMSG : NATS_OP_MSG,
		.sid = sid,
		.data = p->buf + next + hdr_len,
		.data_len = total_len - hdr_len,
	};

	if (hmsg) {
		op->hdr = p->buf + next;
		op->hdr_len = hdr_len;
	}

	if (ntok == want + 1) {
		op->reply = terminate(p, &tok[2]);
		op->reply_len = tok[2].len;
	} else {
		op->reply = (char *)p->buf + line_end;
		op->reply_len = 0;
	}

	op->subject = terminate(p, &tok[0]);
	op->subject_len = tok[0].len;
	p->buf[line_end] = '\0';
	p->buf[frame - 2] = '\0';
	p->consumed = frame;

	return 1;
}

int nats_parser_next(struct nats_parser *p, struct nats_op *op)
{
	uint8_t *nl;
	size_t line_end;
	size_t next;
	size_t name_end;
	size_t args;

	compact(p);

	for (;;) {
		if (p->len == 0) {
			return 0;
		}

		nl = memchr(p->buf, '\n', p->len);
		if (nl == NULL) {
			return p->len == p->size ? -EMSGSIZE : 0;
		}

		next = nl - p->buf + 1;
		line_end = next - 1;
		if (line_end > 0 && p->buf[line_end - 1] == '\r') {
			line_end--;
		}

		if (line_end > 0) {
			break;
		}

		/* Tolerate blank lines between operations. */
		discard(p, next);
	}

	name_end = 0;
	while (name_end < line_end && !is_space(p->buf[name_end])) {
		name_end++;
	}

	args = name_end;
	while (args < line_end && is_space(p->buf[args])) {
		args++;
	}

	if (op_is(p->buf, name_end, "MSG")) {
		return parse_msg(p, op, false, args, line_end, next);
	}

	if (op_is(p->buf, name_end, "HMSG")) {
		return parse_msg(p, op, true, args, line_end, next);
	}

	*op = (struct nats_op){0};

	if (op_is(p->buf, name_end, "PING")) {
		op->type = NATS_OP_PING;
	} else if (op_is(p->buf, name_end, "PONG")) {
		op->type = NATS_OP_PONG;
	} else if (op_is(p->buf, name_end, "+OK")) {
		op->type = NATS_OP_OK;
	} else if (op_is(p->buf, name_end, "INFO")) {
		op->type = NATS_OP_INFO;
	} else if (op_is(p->buf, name_end, "-ERR")) {
		op->type = NATS_OP_ERR;
		/* Strip the quotes around the error text. */
		if (line_end - args >= 2 && p->buf[args] == '\'' &&
		    p->buf[line_end - 1] == '\'') {
			args++;
			line_end--;
		}
	} else {
		return -EBADMSG;
	}

	p->buf[line_end] = '\0';
	op->args = (char *)p->buf + args;
	op->args_len = line_end - args;
	p->consumed = next;

	return 1;
}
