import amqp from "amqplib";
import { declareAndBind, SimpleQueueType } from "./declareAndBind.js";

export async function subscribeJSON<T>(
	conn: amqp.ChannelModel,
	exchange: string,
	queueName: string,
	key: string,
	queueType: SimpleQueueType,
	handler: (data: T) => void,
): Promise<void> {
	// call declare and bind to make sure queue exists
	const [channel, queue] = await declareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	);
	// Use new channel to call consume method
	await channel.consume(queue.queue, (message: amqp.ConsumeMessage | null) => {
		if (!message) return;
		const stringifiedBuffer = message.content.toString();
		const parsedMessage = JSON.parse(stringifiedBuffer);
		handler(parsedMessage);
		channel.ack(message);
	});
}
