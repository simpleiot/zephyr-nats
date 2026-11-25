/*
 * Copyright (c) 2026 Cliff Brake
 * SPDX-License-Identifier: Apache-2.0
 *
 * Publish a counter once a second on zephyr.sample.count, and log and answer
 * messages on zephyr.sample.cmd.
 */

#include <zephyr/kernel.h>
#include <zephyr/logging/log.h>
#include <zephyr/net/nats.h>

LOG_MODULE_REGISTER(nats_pubsub, LOG_LEVEL_INF);

static struct nats_client client;
static struct nats_sub cmd_sub;

static void on_cmd(struct nats_client *c, const struct nats_msg *msg, void *user_data)
{
	ARG_UNUSED(user_data);

	LOG_INF("%s: %s", msg->subject, (const char *)msg->data);

	if (msg->reply_len > 0) {
		nats_respond(c, msg, "ok", 2);
	}
}

static void on_event(struct nats_client *c, enum nats_event event, const char *err,
		     void *user_data)
{
	ARG_UNUSED(c);
	ARG_UNUSED(user_data);

	switch (event) {
	case NATS_EVENT_CONNECTED:
	case NATS_EVENT_RECONNECTED:
		LOG_INF("connected");
		break;
	case NATS_EVENT_DISCONNECTED:
		LOG_INF("disconnected");
		break;
	case NATS_EVENT_ERROR:
		LOG_WRN("server error: %s", err);
		break;
	}
}

int main(void)
{
	const struct nats_config cfg = {
		.url = CONFIG_SAMPLE_NATS_URL,
		.name = "zephyr-pubsub",
		.event_cb = on_event,
	};
	char buf[12];
	int ret;

	ret = nats_client_init(&client, &cfg);
	if (ret < 0) {
		LOG_ERR("invalid config (%d)", ret);
		return 0;
	}

	/* Subscriptions made before connecting are sent once connected. */
	nats_subscribe(&client, &cmd_sub, "zephyr.sample.cmd", NULL, on_cmd, NULL);

	LOG_INF("connecting to %s", CONFIG_SAMPLE_NATS_URL);
	if (nats_connect(&client, K_SECONDS(10)) == -EAGAIN) {
		LOG_INF("not connected yet; still trying");
	}

	for (uint32_t count = 0;; count++) {
		int n = snprintk(buf, sizeof(buf), "%u", count);

		ret = nats_publish(&client, "zephyr.sample.count", buf, n);
		if (ret < 0 && ret != -ENOTCONN) {
			LOG_WRN("publish failed (%d)", ret);
		}
		k_sleep(K_SECONDS(1));
	}

	return 0;
}
