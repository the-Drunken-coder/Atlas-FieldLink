export interface TransferSenderSession {
  readonly fragmentCount: number;
  readonly signal?: AbortSignal;
  open(timeoutMs: number): Promise<void>;
  sendFragment(index: number, retransmission: boolean): Promise<void>;
  requestReceipt(
    windowStart: number,
    windowCount: number,
    timeoutMs: number,
  ): Promise<number>;
  waitForCompletion(timeoutMs: number): Promise<void>;
}

export interface TransferReceiverState {
  receipt(
    windowStart: number,
    windowCount: number,
    hasFragment: (index: number) => boolean,
  ): number;
}

export interface RetryResult {
  readonly retransmissions: number;
  readonly receipts: number;
}

export interface RetrySender {
  run(session: TransferSenderSession): Promise<RetryResult>;
}

export interface RetryStrategy {
  readonly id: number;
  readonly name: string;
  createSender(): RetrySender;
  createReceiver(): TransferReceiverState;
}

export class RetryExhaustedError extends Error {}
export class TransferRejectedError extends Error {}
