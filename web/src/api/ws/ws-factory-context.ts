import { createContext } from "react";
import type { WebSocketFactory } from "@/api/ws/transport";

/**
 * Optional injection point for tests. Production leaves it null and
 * `GatewayWsTransport` uses the browser WebSocket constructor.
 */
export const WsFactoryContext = createContext<WebSocketFactory | null>(null);
