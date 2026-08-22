export interface RunSignals {
  readonly signal: AbortSignal;
  handle(signal: NodeJS.Signals): void;
  beginFinalization(): void;
}

export function createRunSignals(
  record: (type: string, data: unknown) => void,
  warn: (message: string) => void,
): RunSignals {
  const controller = new AbortController();
  let finalizing = false;

  return {
    signal: controller.signal,
    handle(signal: NodeJS.Signals) {
      if (finalizing) {
        warn(`\n${signal}: artifact finalization already in progress\n`);
        return;
      }
      if (!controller.signal.aborted) {
        record("interrupted", { signal });
        controller.abort(signal);
        warn(`\n${signal}: stopping after the active radio operation\n`);
      }
    },
    beginFinalization() {
      finalizing = true;
    },
  };
}
