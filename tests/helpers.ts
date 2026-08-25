import type { FieldLinkTransport, TransportDatagram } from "../src/node.js";

export class MemoryTransport implements FieldLinkTransport {
  readonly sent: Uint8Array[] = [];
  readonly queueLengths: number[] = [];
  readonly listenerErrors: Error[] = [];
  readonly #listeners = new Set<
    (datagram: TransportDatagram) => void | Promise<void>
  >();
  #deliveryTail: Promise<void> = Promise.resolve();
  peer: MemoryTransport | undefined;
  drop: ((bytes: Uint8Array, sequence: number) => boolean) | undefined;
  queueLength = 0;
  closed = false;

  send(bytes: Uint8Array): Promise<void> {
    if (this.closed) {
      throw new Error("transport closed");
    }
    const copy = bytes.slice();
    this.sent.push(copy);
    if (
      this.drop?.(copy, this.sent.length) === true ||
      this.peer === undefined
    ) {
      return Promise.resolve();
    }
    this.peer.queueDelivery({ bytes: copy, snrDb: -7, pathLength: 0xff });
    return Promise.resolve();
  }

  getQueueLength(): Promise<number> {
    this.queueLengths.push(this.queueLength);
    return Promise.resolve(this.queueLength);
  }

  onDatagram(
    listener: (datagram: TransportDatagram) => void | Promise<void>,
  ): () => void {
    this.#listeners.add(listener);
    return () => {
      this.#listeners.delete(listener);
    };
  }

  close(): Promise<void> {
    this.closed = true;
    return Promise.resolve();
  }

  inject(datagram: TransportDatagram): void {
    this.queueDelivery(datagram);
  }

  async settle(): Promise<void> {
    for (;;) {
      const tail = this.#deliveryTail;
      await tail;
      if (tail === this.#deliveryTail) {
        return;
      }
    }
  }

  private queueDelivery(datagram: TransportDatagram): void {
    this.#deliveryTail = this.#deliveryTail.then(async () => {
      for (const listener of this.#listeners) {
        await Promise.resolve(
          listener({ ...datagram, bytes: datagram.bytes.slice() }),
        ).catch((error: unknown) => {
          this.listenerErrors.push(
            error instanceof Error ? error : new Error(String(error)),
          );
        });
      }
    });
  }
}

export function memoryTransportPair(): readonly [
  MemoryTransport,
  MemoryTransport,
] {
  const a = new MemoryTransport();
  const b = new MemoryTransport();
  a.peer = b;
  b.peer = a;
  return [a, b];
}

export async function eventually(
  predicate: () => boolean,
  timeoutMs = 1000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) {
      throw new Error("Condition was not met before timeout");
    }
    await new Promise<void>((resolve) => setTimeout(resolve, 1));
  }
}
