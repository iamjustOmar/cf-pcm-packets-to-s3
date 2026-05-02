export type Packet = {
  sequenceNumber: number;
  timestamp: number;
  payload: number[];
};

export type PacketDto = {
  trackId: string;
  userId: string;
  packet: Packet;
  unixTimestamp: number;
};
