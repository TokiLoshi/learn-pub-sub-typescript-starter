import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import {
	declareAndBind,
	SimpleQueueType,
} from "../internal/pubsub/declareAndBind.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";

async function main() {
	console.log("Starting Peril client...");
	const connectionString = "amqp://guest:guest@localhost:5672/";
	const conn = await amqp.connect(connectionString);
	console.log("Connection success");

	const userName = await clientWelcome();
	const [channel, queue] = await declareAndBind(
		conn,
		ExchangePerilDirect,
		`pause.${userName}`,
		PauseKey,
		SimpleQueueType.Transient,
	);

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
