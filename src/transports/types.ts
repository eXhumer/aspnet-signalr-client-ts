import type { Dispatcher } from 'undici'

export type TransportName = 'webSockets' | 'serverSentEvents' | 'longPolling'

export type ConnectEndpoint = 'connect' | 'reconnect' | 'poll'

export type ConnectRequest = {
  url: string
  /** Form encoded body, only for requests made with `post`. */
  body: string | undefined
}

/**
 * What the client tells a long polling transport about a received payload.
 */
export type DataResult = {
  /** The server asks the client to reconnect (`T` in the response). */
  shouldReconnect?: boolean
  /** Delay in ms before the next poll (`L` in the response). */
  delay?: number
}

/**
 * What a transport needs from the client that owns it.
 */
export interface TransportHost {
  readonly dispatcher: Dispatcher
  /** Request headers, including the bearer token when there is one. */
  readonly headers: Record<string, string>
  /** Timeout (ms) for requests and for opening a connection. */
  readonly requestTimeout: number
  /** Timeout (ms) of a long poll: `ConnectionTimeout` + 10 seconds. */
  readonly pollTimeout: number
  /** Wait (ms) before long polling polls again after a failed poll. */
  readonly longPollReconnectDelay: number
  /** `true` until the connection is up: a failing transport must fall back. */
  readonly connecting: boolean
  /** Number of reconnect attempts made so far. */
  readonly reconnectAttempt: number

  /**
   * URL (and body) of a connect / reconnect / poll request. With `post` the
   * message id and groups token go in the body instead of the URL.
   */
  request(
    endpoint: ConnectEndpoint,
    transport: TransportName,
    post: boolean
  ): ConnectRequest
  /** URL of a `/send` request. */
  sendUrl(transport: TransportName): string

  /** The transport is connected (socket open / stream or first poll OK). */
  onOpen(): void
  /** A raw payload (JSON document) was received. */
  onData(raw: string): DataResult | undefined
  /** Bytes were received, even if they don't carry a payload. */
  onActivity(): void
  /** The transport is gone. Never called after `close()`. */
  onClose(error?: Error): void
  /**
   * Whether a failing transport may retry by itself: the connection is up
   * and was active within the `DisconnectTimeout`.
   */
  canRetry(): boolean
  /** A recoverable transport error (not raised while reconnecting). */
  onError(error: Error): void
  /** The transport moved to the reconnecting state on its own (long polling). */
  onReconnecting(): void
  /** The transport reconnected on its own (long polling). */
  onReconnected(): void
}

export interface Transport {
  readonly name: TransportName
  /** Whether the server sends keep-alives on this transport. */
  readonly supportsKeepAlive: boolean
  /** `true` once the transport can carry client-to-server messages. */
  readonly ready: boolean
  /**
   * Open the transport. With `resume` the `/reconnect` endpoint is used
   * instead of `/connect`.
   */
  connect(resume: boolean): void
  /**
   * Send a payload. Resolves with the response body for transports that
   * deliver invocation results in the response (`undefined` otherwise).
   */
  send(payload: string): Promise<string | undefined>
  close(): void
}
