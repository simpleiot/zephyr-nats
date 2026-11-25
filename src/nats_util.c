/*
 * Copyright (c) 2026 Cliff Brake
 * SPDX-License-Identifier: Apache-2.0
 */

#include <errno.h>
#include <stdio.h>
#include <string.h>

#include "nats_util.h"

static int parse_port(const char *s, size_t len, uint16_t *port)
{
	uint32_t v = 0;

	if (len == 0 || len > 5) {
		return -EINVAL;
	}

	for (size_t i = 0; i < len; i++) {
		if (s[i] < '0' || s[i] > '9') {
			return -EINVAL;
		}
		v = v * 10 + (s[i] - '0');
	}

	if (v == 0 || v > UINT16_MAX) {
		return -EINVAL;
	}

	*port = v;
	return 0;
}

int nats_url_parse(const char *url, struct nats_url *u)
{
	const char *p = url;
	const char *end;
	const char *at;
	const char *host;
	const char *host_end;
	const char *port = NULL;

	memset(u, 0, sizeof(*u));
	u->port = NATS_DEFAULT_PORT;

	if (url == NULL) {
		return -EINVAL;
	}

	if (strncmp(p, "nats://", 7) == 0) {
		p += 7;
	} else if (strncmp(p, "tls://", 6) == 0) {
		u->tls = true;
		p += 6;
	} else if (strstr(p, "://") != NULL) {
		return -EINVAL;
	}

	end = p + strcspn(p, "/");
	if (*end == '/' && end[1] != '\0') {
		return -EINVAL;
	}

	/* Credentials end at the last '@', since a password may contain one. */
	at = NULL;
	for (const char *c = p; c < end; c++) {
		if (*c == '@') {
			at = c;
		}
	}

	if (at != NULL) {
		const char *colon = memchr(p, ':', at - p);

		if (colon != NULL) {
			u->user = p;
			u->user_len = colon - p;
			u->pass = colon + 1;
			u->pass_len = at - colon - 1;
		} else {
			u->token = p;
			u->token_len = at - p;
		}
		p = at + 1;
	}

	if (*p == '[') {
		host = p + 1;
		host_end = memchr(host, ']', end - host);
		if (host_end == NULL) {
			return -EINVAL;
		}
		if (host_end + 1 < end) {
			if (host_end[1] != ':') {
				return -EINVAL;
			}
			port = host_end + 2;
		}
	} else {
		host = p;
		host_end = memchr(host, ':', end - host);
		if (host_end != NULL) {
			port = host_end + 1;
		} else {
			host_end = end;
		}
	}

	if (host_end == host || (size_t)(host_end - host) >= sizeof(u->host)) {
		return -EINVAL;
	}

	memcpy(u->host, host, host_end - host);
	u->host[host_end - host] = '\0';

	if (port != NULL && parse_port(port, end - port, &u->port) < 0) {
		return -EINVAL;
	}

	return 0;
}

bool nats_subject_valid(const char *subject, bool wildcards)
{
	const char *tok = subject;

	if (subject == NULL || *subject == '\0') {
		return false;
	}

	for (;;) {
		size_t len = strcspn(tok, ".");

		if (len == 0) {
			return false;
		}

		for (size_t i = 0; i < len; i++) {
			unsigned char c = tok[i];

			if (c <= ' ' || c == 0x7f) {
				return false;
			}
		}

		if (len == 1 && (tok[0] == '*' || tok[0] == '>')) {
			if (!wildcards) {
				return false;
			}
			if (tok[0] == '>' && tok[1] != '\0') {
				return false;
			}
		}

		if (tok[len] == '\0') {
			return true;
		}

		tok += len + 1;
	}
}

static bool is_ws(char c)
{
	return c == ' ' || c == '\t' || c == '\r' || c == '\n';
}

/* Return the index just past the string that starts at js[i] == '"'. */
static size_t skip_string(const char *js, size_t len, size_t i)
{
	for (i++; i < len; i++) {
		if (js[i] == '\\') {
			i++;
		} else if (js[i] == '"') {
			return i + 1;
		}
	}

	return len;
}

/* Return the index just past the value that starts at js[i]. */
static size_t skip_value(const char *js, size_t len, size_t i)
{
	int depth = 0;

	if (i < len && js[i] == '"') {
		return skip_string(js, len, i);
	}

	while (i < len) {
		char c = js[i];

		if (c == '"') {
			i = skip_string(js, len, i);
			continue;
		}
		if (c == '{' || c == '[') {
			depth++;
		} else if (c == '}' || c == ']') {
			if (depth == 0) {
				return i;
			}
			if (--depth == 0) {
				return i + 1;
			}
		} else if (depth == 0 && (c == ',' || is_ws(c))) {
			return i;
		}
		i++;
	}

	return len;
}

int nats_json_get(const char *js, size_t len, const char *key, const char **val, size_t *val_len)
{
	size_t klen = strlen(key);
	size_t i = 0;
	int depth = 0;

	while (i < len) {
		char c = js[i];

		if (c == '"') {
			size_t start = i + 1;
			size_t j;

			i = skip_string(js, len, i);
			if (depth != 1) {
				continue;
			}

			j = i;
			while (j < len && is_ws(js[j])) {
				j++;
			}
			if (j >= len || js[j] != ':') {
				continue;
			}

			j++;
			while (j < len && is_ws(js[j])) {
				j++;
			}

			if (i - 1 - start == klen && memcmp(js + start, key, klen) == 0) {
				*val = js + j;
				*val_len = skip_value(js, len, j) - j;
				return 0;
			}

			i = skip_value(js, len, j);
			continue;
		}

		if (c == '{' || c == '[') {
			depth++;
		} else if (c == '}' || c == ']') {
			if (--depth <= 0) {
				break;
			}
		}
		i++;
	}

	return -ENOENT;
}

int nats_json_get_uint(const char *json, size_t len, const char *key, uint32_t *out)
{
	const char *v;
	size_t vlen;
	uint64_t n = 0;

	if (nats_json_get(json, len, key, &v, &vlen) < 0 || vlen == 0) {
		return -ENOENT;
	}

	for (size_t i = 0; i < vlen; i++) {
		if (v[i] < '0' || v[i] > '9') {
			return -EINVAL;
		}
		n = n * 10 + (v[i] - '0');
		if (n > UINT32_MAX) {
			return -EINVAL;
		}
	}

	*out = n;
	return 0;
}

int nats_json_get_bool(const char *json, size_t len, const char *key, bool *out)
{
	const char *v;
	size_t vlen;

	if (nats_json_get(json, len, key, &v, &vlen) < 0) {
		return -ENOENT;
	}

	if (vlen == 4 && memcmp(v, "true", 4) == 0) {
		*out = true;
	} else if (vlen == 5 && memcmp(v, "false", 5) == 0) {
		*out = false;
	} else {
		return -EINVAL;
	}

	return 0;
}

int nats_json_escape(char *dst, size_t size, const char *src, size_t len)
{
	size_t n = 0;

	for (size_t i = 0; i < len; i++) {
		unsigned char c = src[i];
		char esc[7];
		size_t elen;

		if (c == '"' || c == '\\') {
			esc[0] = '\\';
			esc[1] = c;
			elen = 2;
		} else if (c < 0x20) {
			snprintf(esc, sizeof(esc), "\\u%04x", c);
			elen = 6;
		} else {
			esc[0] = c;
			elen = 1;
		}

		if (n + elen > size) {
			return -ENOMEM;
		}
		memcpy(dst + n, esc, elen);
		n += elen;
	}

	return n;
}

static int append(char *dst, size_t size, size_t *n, const char *s, size_t len)
{
	if (*n + len > size) {
		return -ENOMEM;
	}

	memcpy(dst + *n, s, len);
	*n += len;
	return 0;
}

int nats_hdr_encode(char *dst, size_t size, const struct nats_hdr *hdrs, size_t count)
{
	size_t n = 0;
	int ret;

	ret = append(dst, size, &n, "NATS/1.0\r\n", 10);

	for (size_t i = 0; ret == 0 && i < count; i++) {
		const char *k = hdrs[i].key;
		const char *v = hdrs[i].value;

		if (k == NULL || v == NULL || *k == '\0' || k[strcspn(k, ": \t\r\n")] != '\0' ||
		    v[strcspn(v, "\r\n")] != '\0') {
			return -EINVAL;
		}

		ret = append(dst, size, &n, k, strlen(k));
		if (ret == 0) {
			ret = append(dst, size, &n, ": ", 2);
		}
		if (ret == 0) {
			ret = append(dst, size, &n, v, strlen(v));
		}
		if (ret == 0) {
			ret = append(dst, size, &n, "\r\n", 2);
		}
	}

	if (ret == 0) {
		ret = append(dst, size, &n, "\r\n", 2);
	}

	return ret < 0 ? ret : (int)n;
}

/* Return the length of the line at p, excluding its CRLF, and set *next past it. */
static size_t line_len(const uint8_t *p, const uint8_t *end, const uint8_t **next)
{
	const uint8_t *nl = memchr(p, '\n', end - p);
	const uint8_t *e;

	if (nl == NULL) {
		*next = end;
		e = end;
	} else {
		*next = nl + 1;
		e = nl;
	}

	if (e > p && e[-1] == '\r') {
		e--;
	}

	return e - p;
}

uint16_t nats_hdr_status(const uint8_t *hdr, size_t len)
{
	const uint8_t *next;
	size_t n = line_len(hdr, hdr + len, &next);
	uint16_t status = 0;
	size_t i = 8;

	if (n < 8 || memcmp(hdr, "NATS/1.0", 8) != 0) {
		return 0;
	}

	while (i < n && hdr[i] == ' ') {
		i++;
	}

	for (int digits = 0; i < n && digits < 3 && hdr[i] >= '0' && hdr[i] <= '9'; digits++) {
		status = status * 10 + (hdr[i++] - '0');
	}

	return status;
}

int nats_hdr_get(const uint8_t *hdr, size_t len, const char *key, const char **val,
		 size_t *val_len)
{
	const uint8_t *end = hdr + len;
	const uint8_t *p = hdr;
	const uint8_t *next;
	size_t klen = strlen(key);

	/* Skip the "NATS/1.0" line. */
	line_len(p, end, &next);
	p = next;

	while (p < end) {
		size_t n = line_len(p, end, &next);

		if (n == 0) {
			break;
		}

		if (n > klen && p[klen] == ':' && memcmp(p, key, klen) == 0) {
			const uint8_t *v = p + klen + 1;
			const uint8_t *ve = p + n;

			while (v < ve && (*v == ' ' || *v == '\t')) {
				v++;
			}
			*val = (const char *)v;
			*val_len = ve - v;
			return 0;
		}

		p = next;
	}

	return -ENOENT;
}
