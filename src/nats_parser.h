/*
 * Copyright (c) 2026 Cliff Brake
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef NATS_PARSER_H_
#define NATS_PARSER_H_

#include <stddef.h>
#include <stdint.h>

#include <zephyr/net/nats.h>

/*
 * Incremental parser for the server side of the NATS client protocol.
 *
 * The parser owns no memory: it works in a buffer the caller provides. The
 * caller reads from the socket into the space nats_parser_space() returns,
 * reports the bytes read with nats_parser_commit(), and then calls
 * nats_parser_next() until it returns 0.
 *
 * An operation returned by nats_parser_next() points into the buffer and
 * stays valid until the next call to any nats_parser_* function. Subjects,
 * reply subjects, arguments, and message payloads are NUL-terminated in place.
 *
 * A message that cannot fit in the buffer is reported as NATS_OP_DROPPED and
 * its payload is discarded as it arrives, so the stream stays in sync.
 */

enum nats_op_type {
	NATS_OP_INFO,
	NATS_OP_MSG,
	NATS_OP_HMSG,
	NATS_OP_PING,
	NATS_OP_PONG,
	NATS_OP_OK,
	NATS_OP_ERR,
	NATS_OP_DROPPED,
};

struct nats_op {
	enum nats_op_type type;
	/* INFO: the JSON object. -ERR: the error text without quotes. */
	char *args;
	size_t args_len;
	/* MSG, HMSG, and DROPPED */
	char *subject;
	size_t subject_len;
	char *reply;
	size_t reply_len;
	uint32_t sid;
	/* HMSG: the header block, from "NATS/1.0" to the blank line */
	uint8_t *hdr;
	size_t hdr_len;
	/* MSG and HMSG: the payload. DROPPED: data is NULL, data_len is the size. */
	uint8_t *data;
	size_t data_len;
};

void nats_parser_init(struct nats_parser *p, uint8_t *buf, size_t size);

/* Discard all buffered data, for example after a reconnect. */
void nats_parser_reset(struct nats_parser *p);

/* Return the free space at the end of the buffer and set *ptr to its start. */
size_t nats_parser_space(struct nats_parser *p, uint8_t **ptr);

/* Report that n bytes were written to the space nats_parser_space() returned. */
void nats_parser_commit(struct nats_parser *p, size_t n);

/*
 * Parse the next operation.
 *
 * Returns 1 when *op holds an operation, 0 when more data is needed,
 * -EMSGSIZE when a protocol line is longer than the buffer, and -EBADMSG for
 * any other protocol error. After an error the parser must be reset.
 */
int nats_parser_next(struct nats_parser *p, struct nats_op *op);

#endif /* NATS_PARSER_H_ */
