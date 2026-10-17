
/**
 * Client method invocation sent by the server.
 */
export type Message = {
  /** Hub name */
  H: string
  /** Method name */
  M: string
  /** Arguments */
  A: any
  /** Hub state */
  S?: Record<string, unknown>
}

/**
 * Persistent response / hub result, using the protocol's field names.
 */
export type SignalRMessage = {
  /** Message id (absent on keep-alives) */
  C?: string
  /** Messages */
  M?: Message[]
  /** Init message flag */
  S?: number | Record<string, unknown>
  /** Groups token */
  G?: string
  /** Server asks the client to reconnect (long polling) */
  T?: number | string
  /** Delay between two polls in ms (long polling) */
  L?: number
  /** Invocation id */
  I?: string | number
  /** Error */
  E?: string
  /** `true` when the error is a hub error */
  H?: boolean
  /** Additional hub error data */
  D?: unknown
  /** Invocation result */
  R?: unknown
  /** Progress of a long running invocation */
  P?: { I: string | number; D: unknown }
}

export type NegotiateProtocol = {
  Url?: string
  ConnectionToken: string
  ConnectionId: string
  /** Seconds. `null` when keep-alives are disabled on the server. */
  KeepAliveTimeout?: number | null
  /** Seconds the client may keep trying to reconnect before giving up. */
  DisconnectTimeout?: number
  /** Seconds a transport may take to connect (until the init message). */
  TransportConnectTimeout?: number
  /** Seconds. Long polls are re-issued after this plus 10 seconds. */
  ConnectionTimeout?: number
  TryWebSockets: boolean
  ProtocolVersion?: string
  WebSocketServerUrl?: string | null
  /** Protocol 2.0+: negotiate again against this URL. */
  RedirectUrl?: string
  /** Protocol 2.0+: bearer token to use with the redirected endpoint. */
  AccessToken?: string
  /** Protocol 2.0+: server side error. */
  Error?: string
  /** Only sent by ASP.NET Core servers. */
  availableTransports?: unknown
}
