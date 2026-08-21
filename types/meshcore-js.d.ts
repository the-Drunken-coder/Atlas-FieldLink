declare module "@liamcottle/meshcore.js" {
  export interface MeshCoreChannelData {
    readonly snr: number;
    readonly channelIdx: number;
    readonly pathLen: number;
    readonly dataType: number;
    readonly dataLen: number;
    readonly data: Uint8Array;
  }

  export interface MeshCoreChannelInfo {
    readonly channelIdx: number;
    readonly name: string;
    readonly secret: Uint8Array;
  }

  export type MeshCoreWaitingMessage =
    | { readonly channelData: MeshCoreChannelData }
    | { readonly channelMessage: unknown }
    | { readonly contactMessage: unknown };

  type MeshCoreListener = (...arguments_: readonly unknown[]) => void;

  export class NodeJSSerialConnection {
    constructor(path: string);

    connect(): Promise<void>;
    close(): Promise<void>;
    on(eventName: string | number, listener: MeshCoreListener): this;
    once(eventName: string | number, listener: MeshCoreListener): this;
    off(eventName: string | number, listener: MeshCoreListener): this;
    getChannel(channelIndex: number): Promise<MeshCoreChannelInfo>;
    sendChannelData(
      channelIndex: number,
      pathLength: number,
      path: Uint8Array,
      dataType: number,
      payload: Uint8Array,
    ): Promise<void>;
    syncNextMessage(): Promise<MeshCoreWaitingMessage | null>;
  }

  export const Constants: {
    readonly DataTypes: {
      readonly Dev: number;
    };
    readonly PushCodes: {
      readonly MsgWaiting: number;
    };
  };
}
