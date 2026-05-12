import { GameState } from "../internal/gamelogic/gamestate.js";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import type { ArmyMove } from "../internal/gamelogic/gamedata.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => void {
	return function (ps: PlayingState) {
		handlePause(gs, ps);
		process.stdout.write("> ");
	};
}

export function handlerMove(gs: GameState): (move: ArmyMove) => void {
	return function (move: ArmyMove) {
		handleMove(gs, move);
		process.stdout.write("> ");
	};
}
