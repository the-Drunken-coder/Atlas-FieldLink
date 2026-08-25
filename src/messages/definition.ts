import type { NodeId, Priority } from "../node-types.js";

export interface MessageHandlerContext {
  readonly source: NodeId;
  readonly destination: NodeId;
  readonly receivedAt: Date;
  reply(message: SupportedMessageLike, priority?: Priority): Promise<void>;
}

export interface MessageExercise<Message> {
  readonly defaultPayloadBytes: number;
  readonly maximumPayloadBytes: number;
  readonly payloadPresets: readonly number[];
  create(payloadBytes: number): Message;
  key(message: Message): string;
  isComplete(input: {
    readonly sent: Message;
    readonly received: Message;
    readonly side: "source" | "destination";
  }): boolean;
}

export interface MessageDefinition<Message> {
  readonly id: number;
  readonly name: string;
  readonly defaultPriority: Priority;
  readonly examples: readonly Message[];
  validate(value: unknown): value is Message;
  encode(message: Message): Uint8Array;
  decode(bytes: Uint8Array): Message;
  readonly exercise: MessageExercise<Message>;
  readonly onMessage?: (
    message: Message,
    context: MessageHandlerContext,
  ) => void | Promise<void>;
}

/** The broad value accepted by a message-local reply without importing the registry. */
export type SupportedMessageLike = Readonly<Record<string, unknown>>;

export class MessageValidationError extends Error {}
