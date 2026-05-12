import { GameState } from "../internal/gamelogic/gamestate.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => void {
	return function (ps: PlayingState) {
		handlePause(gs, ps);
		process.stdout.write("> ");
	};
}
