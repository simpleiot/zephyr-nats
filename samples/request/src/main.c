/*
 * Copyright (c) 2026 Cliff Brake
 * SPDX-License-Identifier: Apache-2.0
 *
 * Send a request on zephyr.sample.echo every two seconds and log the reply.
 * Any NATS responder can answer, for example:
 *
 *   nats reply zephyr.sample.echo 'hello from the server'
 */

#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>
#include <zephyr/net/nats.h>

LOG_MODULE_REGISTER(nats_request, LOG_LEVEL_INF);

static struct nats_client client;

int main(void)
{
	const struct nats_config cfg = {
		.url = CONFIG_SAMPLE_NATS_URL,
		.name = "zephyr-request",
	};
	char reply[64];
	size_t len;
	int ret;

	ret = nats_client_init(&client, &cfg);
	if (ret < 0) {
		LOG_ERR("invalid config (%d)", ret);
		return 0;
	}

	LOG_INF("connecting to %s", CONFIG_SAMPLE_NATS_URL);
	nats_connect(&client, K_SECONDS(10));

	for (;;) {
		ret = nats_request(&client, "zephyr.sample.echo", "ping", 4, reply, sizeof(reply),
				   &len, K_SECONDS(1));
		switch (ret) {
		case 0:
			LOG_INF("reply: %s", reply);
			break;
		case -EHOSTUNREACH:
			LOG_INF("no responders");
			break;
		case -ETIMEDOUT:
			LOG_INF("timed out");
			break;
		case -ENOBUFS:
			LOG_INF("reply truncated from %zu bytes: %s", len, reply);
			break;
		default:
			LOG_INF("request failed (%d)", ret);
			break;
		}
		k_sleep(K_SECONDS(2));
	}

	return 0;
}
