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
import { publishGameLog } from "./index.js";

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
	channel: ConfirmChannel,
): (war: RecognitionOfWar) => Promise<AckType> {
	return async (war: RecognitionOfWar): Promise<AckType> => {
		try {
			const outcome = handleWar(gs, war);
			const userName = gs.getUsername();
			switch (outcome.result) {
				case WarOutcome.NotInvolved:
					return AckType.NackRequeue;

				case WarOutcome.NoUnits: {
					console.log("You're out of units");
					return AckType.NackDiscard;
				}

				// Lose
				case WarOutcome.OpponentWon: {
					console.log("You lost");
					const lostMessage = `${outcome.winner} won a war against ${outcome.loser}`;
					try {
						await publishGameLog(channel, userName, lostMessage);
						return AckType.Ack;
					} catch {
						console.error("Error publishing lost message");
						return AckType.NackRequeue;
					}
				}

				// Win
				case WarOutcome.YouWon: {
					console.log("You won!");
					const wonMessage = `${outcome.winner} won a war against ${outcome.loser}`;
					try {
						await publishGameLog(channel, userName, wonMessage);
						return AckType.Ack;
					} catch (error) {
						console.error("Error publishing won message");
						return AckType.NackRequeue;
					}
				}

				case WarOutcome.Draw: {
					const drawMessage = `A war between ${outcome.attacker} and ${outcome.defender} resulted in a draw`;
					console.log("It's a tie");
					try {
						await publishGameLog(channel, userName, drawMessage);
						return AckType.Ack;
					} catch (error) {
						console.error("Error publishing draw  message");
						return AckType.NackRequeue;
					}
				}

				default: {
					console.error("Uknown war outcome");
					return AckType.NackDiscard;
				}
			}
		} finally {
			process.stdout.write("> ");
		}
	};
}
