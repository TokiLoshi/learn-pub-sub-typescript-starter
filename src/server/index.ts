import amqp, { type ConfirmChannel } from "amqplib";
import process from "node:process";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import {
	ExchangePerilDirect,
	ExchangePerilTopic,
	GameLogSlug,
} from "../internal/routing/routing.js";
import { PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { getInput, printServerHelp } from "../internal/gamelogic/gamelogic.js";
import {
	declareAndBind,
	SimpleQueueType,
} from "../internal/pubsub/declareAndBind.js";
import { writeLog } from "../internal/gamelogic/logs.js";
import { subscribeMsgPack } from "../internal/pubsub/consume.js";
import { handleWrite } from "../server/handlers.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

async function main() {
	console.log("Starting Peril server...");
	const connectionString = "amqp://guest:guest@localhost:5672/";
	const conn = await amqp.connect(connectionString);
	console.log("Connection success");

	const gameState = new GameState("server");
	const playingState: PlayingState = {
		isPaused: gameState.isPaused(),
	};
	const confirm = await conn.createConfirmChannel();

	// const [gameLogChannel, gameLogQueue] = await declareAndBind(
	// 	conn,
	// 	ExchangePerilTopic,
	// 	GameLogSlug,
	// 	`${GameLogSlug}.*`,
	// 	SimpleQueueType.Durable,
	// );
	await subscribeMsgPack(
		conn,
		ExchangePerilTopic,
		GameLogSlug,
		`${GameLogSlug}.*`,
		SimpleQueueType.Durable,
		handleWrite(),
	);

	const json = await publishJSON(
		confirm,
		ExchangePerilDirect,
		PauseKey,
		playingState,
	);

	printServerHelp();

	let running = true;

	while (running) {
		const userInput = await getInput("hi, what would you like to do? \n");
		if (userInput.length === 0) continue;

		const firstWord = userInput[0]?.toLocaleLowerCase().trim();
		// if first word is pause log to console sending a pause message

		if (firstWord === "pause") {
			console.log("Pausing...");
			playingState.isPaused = true;
			// publish the message as before
			await publishJSON(confirm, ExchangePerilDirect, PauseKey, playingState);
			continue;
		}

		if (firstWord === "resume") {
			// if it's resume log to console sending resume
			console.log("Resuming");
			// publish as before but set isPaused to false

			playingState.isPaused = false;
			await publishJSON(confirm, ExchangePerilDirect, PauseKey, playingState);
			continue;
		}

		if (firstWord === "quit") {
			console.log("quitting");
			running = false;
			process.exit(0);
		} else {
			console.log("I don't understand");
			continue;
		}
	}

	const shutdown = async () => {
		console.log("Shutting down...");
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
