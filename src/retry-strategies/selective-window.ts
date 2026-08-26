import {
  RetryExhaustedError,
  TransferRejectedError,
  type RetryResult,
  type RetrySender,
  type RetryStrategy,
  type TransferReceiverState,
  type TransferSenderSession,
} from "../retry.js";

export const SELECTIVE_WINDOW_SIZE = 8;
export const SELECTIVE_WINDOW_REPAIR_ROUNDS = 5;
export const SELECTIVE_WINDOW_RECEIPT_TIMEOUT_MS = 5_000;
const SELECTIVE_WINDOW_CONTROL_TIMEOUT_MS = 30_000;
const SELECTIVE_WINDOW_RECEIPT_REQUEST_ATTEMPTS = 2;

class SelectiveWindowSender implements RetrySender {
  async run(session: TransferSenderSession): Promise<RetryResult> {
    await openTransfer(session);
    let retransmissions = 0;
    let receiptRequests = 0;
    let receiptRequestRetries = 0;
    let receipts = 0;
    const requestReceipt = (windowStart: number, windowCount: number) =>
      requestWindowReceipt(session, windowStart, windowCount, (retry) => {
        receiptRequests += 1;
        if (retry) {
          receiptRequestRetries += 1;
        }
      });

    for (
      let windowStart = 0;
      windowStart < session.fragmentCount;
      windowStart += SELECTIVE_WINDOW_SIZE
    ) {
      const windowCount = Math.min(
        SELECTIVE_WINDOW_SIZE,
        session.fragmentCount - windowStart,
      );
      let missing = fullBitmap(windowCount);
      for (let round = 0; round <= SELECTIVE_WINDOW_REPAIR_ROUNDS; round += 1) {
        throwIfAborted(session.signal);
        for (let offset = 0; offset < windowCount; offset += 1) {
          if ((missing & (1 << offset)) === 0) {
            continue;
          }
          await session.sendFragment(windowStart + offset, round > 0);
          if (round > 0) {
            retransmissions += 1;
          }
        }

        try {
          const received = await requestReceipt(windowStart, windowCount);
          if (received === undefined) {
            return {
              retransmissions,
              receiptRequests,
              receiptRequestRetries,
              receipts,
            };
          }
          receipts += 1;
          missing = fullBitmap(windowCount) & ~received;
          if (missing === 0) {
            break;
          }
        } catch (error: unknown) {
          throwIfAborted(session.signal);
          if (error instanceof TransferRejectedError) {
            throw error;
          }
          if (round === SELECTIVE_WINDOW_REPAIR_ROUNDS) {
            throw new RetryExhaustedError(
              `Selective-window exhausted repairs for fragments ${windowStart}-${windowStart + windowCount - 1}`,
              { cause: error },
            );
          }
        }
      }
      if (missing !== 0) {
        throw new RetryExhaustedError(
          `Selective-window exhausted repairs for fragments ${windowStart}-${windowStart + windowCount - 1}`,
        );
      }
    }

    for (
      let attempt = 0;
      attempt <= SELECTIVE_WINDOW_REPAIR_ROUNDS;
      attempt += 1
    ) {
      try {
        await session.waitForCompletion(SELECTIVE_WINDOW_CONTROL_TIMEOUT_MS);
        return {
          retransmissions,
          receiptRequests,
          receiptRequestRetries,
          receipts,
        };
      } catch (error: unknown) {
        throwIfAborted(session.signal);
        if (error instanceof TransferRejectedError) {
          throw error;
        }
        if (attempt === SELECTIVE_WINDOW_REPAIR_ROUNDS) {
          throw new RetryExhaustedError(
            "Selective-window completion was not received",
            { cause: error },
          );
        }
        const windowStart = Math.max(
          0,
          session.fragmentCount -
            (session.fragmentCount % SELECTIVE_WINDOW_SIZE ||
              SELECTIVE_WINDOW_SIZE),
        );
        const windowCount = session.fragmentCount - windowStart;
        try {
          const received = await requestReceipt(windowStart, windowCount);
          if (received === undefined) {
            return {
              retransmissions,
              receiptRequests,
              receiptRequestRetries,
              receipts,
            };
          }
          receipts += 1;
        } catch (receiptError: unknown) {
          throwIfAborted(session.signal);
          if (receiptError instanceof TransferRejectedError) {
            throw receiptError;
          }
        }
      }
    }
    throw new RetryExhaustedError(
      "Selective-window completion was not received",
    );
  }
}

class SelectiveWindowReceiver implements TransferReceiverState {
  receipt(
    windowStart: number,
    windowCount: number,
    hasFragment: (index: number) => boolean,
  ): number {
    let bitmap = 0;
    for (let offset = 0; offset < windowCount; offset += 1) {
      if (hasFragment(windowStart + offset)) {
        bitmap |= 1 << offset;
      }
    }
    return bitmap;
  }
}

export const selectiveWindowStrategy = {
  id: 1,
  name: "selective-window",
  createSender: () => new SelectiveWindowSender(),
  createReceiver: () => new SelectiveWindowReceiver(),
} satisfies RetryStrategy;

async function openTransfer(session: TransferSenderSession): Promise<void> {
  let lastError: unknown;
  for (
    let attempt = 0;
    attempt <= SELECTIVE_WINDOW_REPAIR_ROUNDS;
    attempt += 1
  ) {
    throwIfAborted(session.signal);
    try {
      await session.open(SELECTIVE_WINDOW_CONTROL_TIMEOUT_MS);
      return;
    } catch (error: unknown) {
      throwIfAborted(session.signal);
      if (error instanceof TransferRejectedError) {
        throw error;
      }
      lastError = error;
    }
  }
  throw new RetryExhaustedError(
    "Receiver did not acknowledge the transfer start",
    {
      cause: lastError,
    },
  );
}

async function requestWindowReceipt(
  session: TransferSenderSession,
  windowStart: number,
  windowCount: number,
  onAttempt: (retry: boolean) => void,
): Promise<number | undefined> {
  let lastError: unknown;
  for (
    let attempt = 0;
    attempt < SELECTIVE_WINDOW_RECEIPT_REQUEST_ATTEMPTS;
    attempt += 1
  ) {
    throwIfAborted(session.signal);
    onAttempt(attempt > 0);
    try {
      return await session.requestReceipt(
        windowStart,
        windowCount,
        SELECTIVE_WINDOW_RECEIPT_TIMEOUT_MS,
      );
    } catch (error: unknown) {
      throwIfAborted(session.signal);
      if (error instanceof TransferRejectedError) {
        throw error;
      }
      lastError = error;
    }
  }
  throw lastError;
}

function fullBitmap(count: number): number {
  return (1 << count) - 1;
}

function throwIfAborted(signal: AbortSignal | undefined): void {
  if (signal?.aborted === true) {
    throw signal.reason instanceof Error
      ? signal.reason
      : new Error("Transfer aborted");
  }
}
