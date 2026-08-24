declare module "@liamcottle/meshcore.js" {
  export interface MeshCoreChannelData {
    readonly snr: number;
    readonly channelIdx: number;
    readonly pathLen: number;
    readonly dataType: number;
    readonly dataLen: number;
    readonly data: Uint8Array;
  }

  export interface MeshCoreChannelMessage {
    readonly channelIdx: number;
    readonly pathLen: number;
    readonly txtType: number;
    readonly senderTimestamp: number;
    readonly text: string;
  }

  export interface MeshCoreContactMessage {
    readonly pubKeyPrefix: Uint8Array;
    readonly pathLen: number;
    readonly txtType: number;
    readonly senderTimestamp: number;
    readonly text: string;
  }

  export interface MeshCoreChannelInfo {
    readonly channelIdx: number;
    readonly name: string;
    readonly secret: Uint8Array;
  }

  export interface MeshCoreSelfInfo {
    readonly publicKey: Uint8Array;
    readonly name: string;
    readonly radioFreq: number;
    readonly radioBw: number;
    readonly radioSf: number;
    readonly radioCr: number;
    readonly txPower: number;
    readonly maxTxPower: number;
  }

  export interface MeshCoreDeviceInfo {
    readonly firmwareVer: number;
    readonly firmware_build_date: string;
    readonly manufacturerModel: string;
  }

  export interface MeshCoreStatsResponse {
    readonly data: {
      readonly batteryMilliVolts: number;
      readonly uptimeSecs: number;
      readonly queueLen: number;
    };
  }

  export type MeshCoreWaitingMessage =
    | { readonly channelData: MeshCoreChannelData }
    | { readonly channelMessage: MeshCoreChannelMessage }
    | { readonly contactMessage: MeshCoreContactMessage };

  export type MeshCoreListener = (...arguments_: readonly unknown[]) => void;

  export class SerialConnection {
    close(): Promise<void>;
    on(eventName: string | number, listener: MeshCoreListener): void;
    once(eventName: string | number, listener: MeshCoreListener): void;
    off(eventName: string | number, listener: MeshCoreListener): void;
    getChannel(channelIndex: number): Promise<MeshCoreChannelInfo>;
    getChannels(): Promise<readonly MeshCoreChannelInfo[]>;
    getSelfInfo(): Promise<MeshCoreSelfInfo>;
    deviceQuery(appTargetVersion: number): Promise<MeshCoreDeviceInfo>;
    getStatsCore(): Promise<MeshCoreStatsResponse>;
    sendChannelData(
      channelIndex: number,
      pathLength: number,
      path: Uint8Array,
      dataType: number,
      payload: Uint8Array,
    ): Promise<void>;
    syncNextMessage(): Promise<MeshCoreWaitingMessage | null>;
    protected onConnected(): Promise<void>;
    protected onDisconnected(): void;
    protected onDataReceived(data: Uint8Array): Promise<void>;
    protected emit(
      eventName: string | number,
      ...arguments_: readonly unknown[]
    ): void;
    protected write(bytes: Uint8Array): Promise<void>;
  }

  export const Constants: {
    readonly SupportedCompanionProtocolVersion: number;
    readonly DataTypes: {
      readonly Dev: number;
    };
    readonly PushCodes: {
      readonly MsgWaiting: number;
    };
  };
}
