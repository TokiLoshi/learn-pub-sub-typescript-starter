import type { ConfirmChannel } from "amqplib";
import { SimpleQueueType } from "./declareAndBind.js";
import pack, { encode } from "@msgpack/msgpack";
import type { GameLog } from "../gamelogic/logs.js";
import { ExchangePerilTopic, GameLogSlug } from "../routing/routing.js";

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

export function publishMsgPack<T>(
	ch: ConfirmChannel,
	exchange: string,
	routingKey: string,
	value: T,
): Promise<void> {
	const body = encode(value);
	return new Promise((resolve, reject) => {
		ch.publish(
			exchange,
			routingKey,
			Buffer.from(body),
			{ contentType: "application/x-msgpack" },
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

export async function publishGameLog(
	channel: ConfirmChannel,
	username: string,
	message: string,
) {
	const newGameLog: GameLog = {
		username: username,
		message: message,
		currentTime: new Date(),
	};
	await publishMsgPack(
		channel,
		ExchangePerilTopic,
		`${GameLogSlug}.${username}`,
		newGameLog,
	);
}
