/*
 * Copyright (c) 2026 Cliff Brake
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef NATS_UTIL_H_
#define NATS_UTIL_H_

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include <zephyr/net/nats.h>

/* Socket-independent helpers, kept apart from the client so they can be tested on their own. */

#define NATS_URL_HOST_MAX  64
#define NATS_DEFAULT_PORT  4222

struct nats_url {
	char host[NATS_URL_HOST_MAX];
	uint16_t port;
	bool tls;
	/* Credentials point into the URL string and are not NUL-terminated. */
	const char *user;
	size_t user_len;
	const char *pass;
	size_t pass_len;
	const char *token;
	size_t token_len;
};

/*
 * Parse "nats://[user:pass@|token@]host[:port]", "tls://...", or "host[:port]".
 * IPv6 hosts are written in brackets. Returns 0 or -EINVAL.
 */
int nats_url_parse(const char *url, struct nats_url *u);

/* Check a subject. Wildcard tokens ("*", and ">" as the last token) are allowed only when asked. */
bool nats_subject_valid(const char *subject, bool wildcards);

/* Find a top-level key in a JSON object and return its raw value. Returns 0 or -ENOENT. */
int nats_json_get(const char *json, size_t len, const char *key, const char **val,
		  size_t *val_len);
int nats_json_get_uint(const char *json, size_t len, const char *key, uint32_t *out);
int nats_json_get_bool(const char *json, size_t len, const char *key, bool *out);

/* Write src as the body of a JSON string. Returns the bytes written or -ENOMEM. */
int nats_json_escape(char *dst, size_t size, const char *src, size_t len);

/* Encode a header block. Returns its length, -ENOMEM, or -EINVAL for an invalid header. */
int nats_hdr_encode(char *dst, size_t size, const struct nats_hdr *hdrs, size_t count);

/* Return the status code on the "NATS/1.0" line of a header block, or 0 if there is none. */
uint16_t nats_hdr_status(const uint8_t *hdr, size_t len);

/* Find the first value of a header, matching the key exactly. Returns 0 or -ENOENT. */
int nats_hdr_get(const uint8_t *hdr, size_t len, const char *key, const char **val,
		 size_t *val_len);

#endif /* NATS_UTIL_H_ */
