import { describe, expect, it, vi } from "vitest";

import { createRunSignals } from "../src/run-signals.js";

describe("run signals", () => {
  it("absorbs signals while artifact finalization is pending", () => {
    const record = vi.fn();
    const warn = vi.fn();
    const signals = createRunSignals(record, warn);

    signals.beginFinalization();
    signals.handle("SIGTERM");

    expect(signals.signal.aborted).toBe(false);
    expect(record).not.toHaveBeenCalled();
    expect(warn).toHaveBeenCalledWith(
      "\nSIGTERM: artifact finalization already in progress\n",
    );
  });
});
