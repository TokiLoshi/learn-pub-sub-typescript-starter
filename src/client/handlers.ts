// import { GameState } from "../internal/gamelogic/gamestate.js";
import amqp from "amqplib";
import type { ConfirmChannel } from "amqplib";
import type {
	GameState,
	PlayingState,
} from "../internal/gamelogic/gamestate.js";
import { handlePause } from "../internal/gamelogic/pause.js";
import type {
	ArmyMove,
	RecognitionOfWar,
} from "../internal/gamelogic/gamedata.js";
import { handleMove, MoveOutcome } from "../internal/gamelogic/move.js";
import { AckType } from "../internal/pubsub/consumer.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import {
	ExchangePerilTopic,
	WarRecognitionsPrefix,
} from "../internal/routing/routing.js";
import { handleWar } from "../internal/gamelogic/war.js";
import { WarOutcome } from "../internal/gamelogic/war.js";

export function handlerPause(gs: GameState): (ps: PlayingState) => AckType {
	return (ps: PlayingState): AckType => {
		handlePause(gs, ps);
		process.stdout.write("> ");
		return AckType.Ack;
	};
}

export function handlerMove(
	gs: GameState,
	publishCh: ConfirmChannel,
): (move: ArmyMove) => Promise<AckType> {
	return async (move: ArmyMove): Promise<AckType> => {
		try {
			const outcome = handleMove(gs, move);
			switch (outcome) {
				case MoveOutcome.Safe:
					return AckType.Ack;
				case MoveOutcome.MakeWar:
					const rw: RecognitionOfWar = {
						attacker: move.player,
						defender: gs.getPlayerSnap(),
					};
					const userName = gs.getUsername();
					try {
						await publishJSON(
							publishCh,
							ExchangePerilTopic,
							`${WarRecognitionsPrefix}.${userName}`,
							rw,
						);

						return AckType.Ack;
					} catch (error) {
						console.error("Error publishing");
						return AckType.NackRequeue;
					}

				default:
					return AckType.NackDiscard;
			}
		} finally {
			process.stdout.write("> ");
		}
	};
}

export function handlerWar(
	gs: GameState,
): (war: RecognitionOfWar) => Promise<AckType> {
	return async (war: RecognitionOfWar): Promise<AckType> => {
		try {
			const outcome = handleWar(gs, war);
			switch (outcome.result) {
				case WarOutcome.NotInvolved:
					return AckType.NackRequeue;
				case WarOutcome.NoUnits:
					console.log("You're out of units");
					return AckType.NackDiscard;
				case WarOutcome.OpponentWon:
					console.log("You lost");
					return AckType.Ack;
				case WarOutcome.YouWon:
					console.log("You won!");
					return AckType.Ack;
				case WarOutcome.Draw:
					console.log("It's a tie");
					return AckType.Ack;
				default:
					console.error("Uknown war outcome");
					return AckType.NackDiscard;
			}
		} finally {
			process.stdout.write("> ");
		}
	};
}
