import type { ConfirmChannel } from "amqplib";

export function publishJSON<T>(
	ch: ConfirmChannel,
	exchange: string,
	routingKey: string,
	value: T,
): Promise<void> {
	const json = JSON.stringify(value);
	const buffer = Buffer.from(json);
	return new Promise((resolve, reject) => {
		ch.publish(
			exchange,
			routingKey,
			buffer,
			{ contentType: "application/json" },
			(err) => {
				if (err !== null) {
					reject(new Error("Message was NACKed by the broker"));
				} else {
					resolve();
				}
			},
		);
	});
}
