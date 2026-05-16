import type {
	GameState,
	PlayingState,
} from "../internal/gamelogic/gamestate.js";
import { writeLog } from "../internal/gamelogic/logs.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import { AckType } from "../internal/pubsub/consumer.js";
import type { GameLog } from "../internal/gamelogic/logs.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
	return (ps: PlayingState): AckType => {
		handlePause(gs, ps);
		process.stdout.write("> ");
		return AckType.Ack;
	};
}

export function handleWrite() {
	return async (data: GameLog): Promise<AckType> => {
		try {
			await writeLog(data);
			return AckType.Ack;
		} catch (err) {
			return AckType.NackDiscard;
		} finally {
			process.stdout.write("> ");
		}
	};
}
