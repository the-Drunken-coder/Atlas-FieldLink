import type { MessageDefinition } from "./definition.js";
import { testMessage } from "./test.js";

export const messageRegistry = [testMessage] as const;

type MessageFromDefinition<Definition> =
  Definition extends MessageDefinition<infer Message> ? Message : never;

export type SupportedMessage = MessageFromDefinition<
  (typeof messageRegistry)[number]
>;
export type MessageName = (typeof messageRegistry)[number]["name"];

validateRegistry(messageRegistry);

export function definitionForMessage(
  message: unknown,
): MessageDefinition<SupportedMessage> {
  const definition = messageRegistry.find((candidate) =>
    candidate.validate(message),
  );
  if (definition === undefined) {
    throw new Error("Unsupported or invalid FieldLink message");
  }
  return definition;
}

export function definitionForType(
  id: number,
): MessageDefinition<SupportedMessage> | undefined {
  return messageRegistry.find((candidate) => candidate.id === id);
}

export function definitionForName(
  name: string,
): MessageDefinition<SupportedMessage> | undefined {
  return messageRegistry.find((candidate) => candidate.name === name);
}

export function validateRegistry(
  registry: readonly MessageDefinition<SupportedMessage>[],
): void {
  const ids = new Set<number>();
  const names = new Set<string>();
  for (const definition of registry) {
    if (
      !Number.isInteger(definition.id) ||
      definition.id < 0 ||
      definition.id > 0xffff
    ) {
      throw new Error(`Message ${definition.name} has an invalid uint16 ID`);
    }
    if (ids.has(definition.id)) {
      throw new Error(`Duplicate message ID ${definition.id}`);
    }
    if (names.has(definition.name)) {
      throw new Error(`Duplicate message name ${definition.name}`);
    }
    validateExercise(definition);
    ids.add(definition.id);
    names.add(definition.name);
  }
}

function validateExercise(
  definition: MessageDefinition<SupportedMessage>,
): void {
  const exercise = definition.exercise;
  if (
    !Number.isInteger(exercise.defaultPayloadBytes) ||
    !Number.isInteger(exercise.maximumPayloadBytes) ||
    exercise.defaultPayloadBytes < 0 ||
    exercise.defaultPayloadBytes > exercise.maximumPayloadBytes
  ) {
    throw new Error(`Message ${definition.name} has invalid exercise bounds`);
  }
  for (const payloadBytes of [
    exercise.defaultPayloadBytes,
    ...exercise.payloadPresets,
  ]) {
    if (
      !Number.isInteger(payloadBytes) ||
      payloadBytes < 0 ||
      payloadBytes > exercise.maximumPayloadBytes
    ) {
      throw new Error(
        `Message ${definition.name} has an invalid exercise payload preset`,
      );
    }
    if (!definition.validate(exercise.create(payloadBytes))) {
      throw new Error(
        `Message ${definition.name} exercise created an invalid message`,
      );
    }
  }
}

export type {
  MessageDefinition,
  MessageExercise,
  MessageHandlerContext,
} from "./definition.js";
export { MessageValidationError } from "./definition.js";
export { testMessage, type TestMessage } from "./test.js";
