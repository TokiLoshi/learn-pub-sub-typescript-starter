import amqp from "amqplib";
import process from "node:process";
import { publishJSON } from "../internal/pubsub/publish.js";
import { ExchangePerilDirect } from "../internal/routing/routing.js";
import { PauseKey } from "../internal/routing/routing.js";
import { GameState } from "../internal/gamelogic/gamestate.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";

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
	const json = await publishJSON(
		confirm,
		ExchangePerilDirect,
		PauseKey,
		playingState,
	);

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
