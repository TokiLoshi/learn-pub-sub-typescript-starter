import { decode } from "@msgpack/msgpack";
import amqp from "amqplib";
import { AckType } from "./consumer.js";
import { declareAndBind, SimpleQueueType } from "./declareAndBind.js";

export async function subscribe<T>(
	conn: amqp.ChannelModel,
	exchange: string,
	queueName: string,
	routingKey: string,
	simpleQueueType: SimpleQueueType,
	handler: (data: T) => Promise<AckType> | AckType,
	deserializer: (data: Buffer) => T,
): Promise<void> {
	const [channel, queue] = await declareAndBind(
		conn,
		exchange,
		queueName,
		routingKey,
		simpleQueueType,
	);
	await channel.consume(
		queue.queue,
		async (message: amqp.ConsumeMessage | null) => {
			if (!message) return;

			try {
				const data = deserializer(message.content);
				const result = await handler(data);
				switch (result) {
					case AckType.Ack: {
						channel.ack(message);
						console.log("Ack");
						break;
					}
					case AckType.NackDiscard: {
						channel.nack(message, false, false);
						console.log("NackDiscard");
						break;
					}
					case AckType.NackRequeue: {
						channel.nack(message, false, true);
						break;
					}
					default: {
						const unreachable: never = result;
						console.error("Unexpected ack type: ", unreachable);
						return;
					}
				}
			} catch (error) {
				console.error("Error handling message: ", error);
				channel.nack(message, false, false);
				return;
			}
		},
	);
}

export async function subscribeMsgPack<T>(
	conn: amqp.ChannelModel,
	exchange: string,
	queueName: string,
	routingKey: string,
	simpleQueueType: SimpleQueueType,
	handler: (data: T) => Promise<AckType> | AckType,
): Promise<void> {
	return subscribe(
		conn,
		exchange,
		queueName,
		routingKey,
		simpleQueueType,
		handler,
		(data) => decode(data),
	);
}
