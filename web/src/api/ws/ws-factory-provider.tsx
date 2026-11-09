import type { ReactNode } from "react";
import { WsFactoryContext } from "@/api/ws/ws-factory-context";
import type { WebSocketFactory } from "@/api/ws/transport";

export function WsFactoryProvider({
  factory,
  children,
}: {
  factory: WebSocketFactory | null;
  children: ReactNode;
}) {
  return (
    <WsFactoryContext.Provider value={factory}>
      {children}
    </WsFactoryContext.Provider>
  );
}
