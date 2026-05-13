import amqp from "amqplib";
import type { ConfirmChannel } from "amqplib";
import {
	clientWelcome,
	commandStatus,
	getInput,
	printClientHelp,
	printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { commandMove, handleMove } from "../internal/gamelogic/move.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { subscribeJSON } from "../internal/pubsub/consumer.js";
import {
	declareAndBind,
	SimpleQueueType,
} from "../internal/pubsub/declareAndBind.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import {
	ArmyMovesPrefix,
	ExchangePerilDirect,
	ExchangePerilTopic,
	GameLogSlug,
	PauseKey,
	WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import { handlerMove, handlerPause, handlerWar } from "./handlers.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

async function main() {
	console.log("Starting Peril client...");
	const connectionString = "amqp://guest:guest@localhost:5672/";
	const conn = await amqp.connect(connectionString);
	console.log("Connection success");

	const publishCh = await conn.createConfirmChannel();

	const userName = await clientWelcome();

	// subscribe to game state
	const gameState = new GameState(userName);
	await subscribeJSON(
		conn,
		ExchangePerilDirect,
		`${PauseKey}.${userName}`,
		PauseKey,
		SimpleQueueType.Transient,
		handlerPause(gameState),
	);

	// subscribe to moves
	await subscribeJSON(
		conn,
		ExchangePerilTopic,
		`${ArmyMovesPrefix}.${userName}`,
		`${ArmyMovesPrefix}.*`,
		SimpleQueueType.Transient,
		handlerMove(gameState, publishCh),
	);

	// subscribe to war
	await subscribeJSON(
		conn,
		ExchangePerilTopic,
		"war",
		`${WarRecognitionsPrefix}.#`,
		SimpleQueueType.Durable,
		handlerWar(gameState, publishCh),
	);

	// create a repl
	let running = true;
	while (running) {
		const userInput = await getInput(
			`hi ${userName}, what would you like to do? \n`,
		);
		if (userInput.length === 0) continue;

		const command = userInput[0];

		if (command === "move") {
			try {
				const move = commandMove(gameState, userInput);
				await publishJSON(
					publishCh,
					ExchangePerilTopic,
					`${ArmyMovesPrefix}.${userName}`,
					move,
				);
				console.log("Move successfully published");
			} catch (error) {
				console.error((error as Error).message);
			}
		} else if (command === "spawn") {
			try {
				commandSpawn(gameState, userInput);
			} catch (error) {
				console.error((error as Error).message);
			}
		} else if (command === "status") {
			try {
				commandStatus(gameState);
			} catch (error) {
				console.error((error as Error).message);
			}
		} else if (command === "help") {
			try {
				printClientHelp();
			} catch (error) {
				console.error((error as Error).message);
			}
		} else if (command === "spam") {
			console.log("Spamming not allowed yet");
		} else if (command === "quit") {
			printQuit();
			running = false;
			process.exit(0);
		} else {
			console.log("unknown command");
		}
	}
	// spawn - add new unit triggers commpandSpa

	const shutdown = async () => {
		console.log("Shutting down....");
		try {
			await conn.close();
		} catch (error) {
			console.log("Error: ", error);
			process.exit(0);
		}
	};
	process.on("SIGINT", shutdown);
	process.on("SIGTERM", shutdown);
}

main().catch((err) => {
	console.error("Fatal error:", err);
	process.exit(1);
});

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
