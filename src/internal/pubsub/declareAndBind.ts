import amqp from "amqplib";
import type { Channel } from "amqplib";
import { DeadLetterExchange } from "../routing/routing.js";

export enum SimpleQueueType {
	Durable,
	Transient,
}

export async function declareAndBind(
	conn: amqp.ChannelModel,
	exchange: string,
	queueName: string,
	key: string,
	queueType: SimpleQueueType,
): Promise<[Channel, amqp.Replies.AssertQueue]> {
	const channel = await conn.createChannel();

	const queue = await channel.assertQueue(queueName, {
		durable: queueType === SimpleQueueType.Durable,
		autoDelete: queueType !== SimpleQueueType.Durable,
		exclusive: queueType !== SimpleQueueType.Durable,
		arguments: {
			"x-dead-letter-exchange": DeadLetterExchange,
		},
	});
	await channel.bindQueue(queue.queue, exchange, key);
	return [channel, queue];
}
