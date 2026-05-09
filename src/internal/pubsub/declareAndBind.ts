import amqp from "amqplib";

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
): Promise<[amqp.Channel, amqp.Replies.AssertQueue]> {
	const channel = await conn.createChannel();
	const isDurable = queueType === SimpleQueueType.Durable;
	const isTransient = queueType === SimpleQueueType.Transient;
	const newQueue = channel.assertQueue(queueName, {
		durable: isDurable,
		autoDelete: isTransient,
		exclusive: isTransient,
	});
	await channel.bindQueue((await newQueue).queue, exchange, key);
	return [channel, newQueue];
}
