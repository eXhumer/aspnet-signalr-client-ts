export type ErrorCode =
  | 'INVALID_URL'
  | 'INVALID_PROTOCOL'
  | 'NO_HUB'
  | 'NO_TRANSPORT'
  | 'UNSUPPORTED_WEBSOCKET'
  | 'UNAUTHORIZED'
  | 'CONNECTION_LOST'
  | 'ERR_NEGOTIATE'
  | 'ERR_PROTOCOL'
  | 'ERR_CONNECT'
  | 'ERR_START'
  | 'ERR_SOCKET'
  | 'ERR_ABORT'
  | 'ERR_SEND'
  | 'ERR_PING'
  | 'ERR_PARSE'
  | 'ERR_SERVER'

/**
 * Create an Error for the signalR client with the specified error code and
 * message.
 */
export class SignalRError extends Error {
  code: ErrorCode

  constructor(code: ErrorCode, message: string) {
    super(message)
    this.name = 'SignalRError'
    this.code = code
  }

  static from(code: ErrorCode, error: Error): SignalRError {
    // Wrap instead of mutating: some errors (e.g. DOMException) have a
    // read-only `code`.
    const _error = new SignalRError(code, error.message)
    _error.cause = error
    return _error
  }
}

/**
 * Error returned by a hub method invocation.
 */
export class HubError extends Error {
  /**
   * `true` when the server flagged the error as a `HubException`.
   */
  readonly isHubError: boolean
  /**
   * Additional error data sent by the server for hub errors.
   */
  readonly data: unknown

  constructor(message: string, isHubError = false, data?: unknown) {
    super(message)
    this.name = 'HubError'
    this.isHubError = isHubError
    this.data = data
  }
}
