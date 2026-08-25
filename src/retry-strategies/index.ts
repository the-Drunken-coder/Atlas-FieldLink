import type { RetryStrategy } from "../retry.js";
import { selectiveWindowStrategy } from "./selective-window.js";

export const retryStrategies = [selectiveWindowStrategy] as const;

validateRetryStrategies(retryStrategies);

export type RetryStrategyName = (typeof retryStrategies)[number]["name"];

export function retryStrategyByName(name: string): RetryStrategy | undefined {
  return retryStrategies.find((strategy) => strategy.name === name);
}

export function retryStrategyById(id: number): RetryStrategy | undefined {
  return retryStrategies.find((strategy) => strategy.id === id);
}

function validateRetryStrategies(strategies: readonly RetryStrategy[]): void {
  const ids = new Set<number>();
  const names = new Set<string>();
  for (const strategy of strategies) {
    if (
      !Number.isInteger(strategy.id) ||
      strategy.id < 0 ||
      strategy.id > 0xff
    ) {
      throw new Error(`Retry strategy ${strategy.name} has an invalid byte ID`);
    }
    if (ids.has(strategy.id) || names.has(strategy.name)) {
      throw new Error(
        `Duplicate retry strategy ${strategy.id}/${strategy.name}`,
      );
    }
    ids.add(strategy.id);
    names.add(strategy.name);
  }
}

export { selectiveWindowStrategy } from "./selective-window.js";
