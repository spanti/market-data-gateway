import { useContext } from "react";
import { WsFactoryContext } from "@/api/ws/ws-factory-context";
import type { WebSocketFactory } from "@/api/ws/transport";

export function useWsFactory(): WebSocketFactory | null {
  return useContext(WsFactoryContext);
}
