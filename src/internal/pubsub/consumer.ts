import amqp from "amqplib";
import { declareAndBind, SimpleQueueType } from "./declareAndBind.js";

export enum AckType {
	Ack,
	NackDiscard,
	NackRequeue,
}

export async function subscribeJSON<T>(
	conn: amqp.ChannelModel,
	exchange: string,
	queueName: string,
	key: string,
	queueType: SimpleQueueType,
	handler: (data: T) => Promise<AckType> | AckType,
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
	await channel.consume(
		queue.queue,
		async (message: amqp.ConsumeMessage | null) => {
			if (!message) return;
			const stringifiedBuffer = message.content.toString();
			const parsedMessage = JSON.parse(stringifiedBuffer);

			try {
				const result = await handler(parsedMessage);
				switch (result) {
					case AckType.Ack:
						channel.ack(message);
						console.log("Ack");
						break;
					case AckType.NackDiscard:
						channel.nack(message, false, false);
						console.log("NackDiscard");
						break;
					case AckType.NackRequeue:
						channel.nack(message, false, true);
						console.log("NackRequeue");
						break;
					default:
						const unreachable: never = result;
						console.error("Unexpected ack type: ", unreachable);
						return;
				}
			} catch (error) {
				console.error("Error handling message: ", error);
				channel.nack(message, false, false);
				return;
			}
		},
	);
}
