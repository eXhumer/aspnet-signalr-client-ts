import { EventEmitter } from 'node:events'
import { cookie } from '@exhumer/undici-cookie-agent'
import { CookieJar } from 'tough-cookie'
import { Agent, request, type Dispatcher } from 'undici'
import { SignalRError, type ErrorCode } from './errors.js'
import { Hub } from './hub.js'
import type { Message, NegotiateProtocol, SignalRMessage } from './messages.js'
import { LongPollingTransport } from './transports/long-polling.js'
import { ServerSentEventsTransport } from './transports/server-sent-events.js'
import { FORM_CONTENT_TYPE } from './transports/shared.js'
import type {
  ConnectEndpoint,
  ConnectRequest,
  DataResult,
  Transport,
  TransportHost,
  TransportName
} from './transports/types.js'
import { WebSocketTransport } from './transports/websocket.js'

export { HubError, SignalRError, type ErrorCode } from './errors.js'
export type { HubEvent } from './hub.js'
export type { TransportName } from './transports/types.js'

export type ClientEvents = {
  connected: () => void
  reconnecting: (retryCount: number) => void
  disconnected: (reason: 'failed' | 'unauthorized' | 'end') => void
  /** No keep-alive was received for 2/3 of the keep-alive timeout. */
  connectionSlow: () => void
  error: (error: SignalRError) => void
}

type ConnectionState =
  | 'Connecting'
  | 'Connected'
  | 'Reconnecting'
  | 'Disconnected'

interface Connection {
  id?: string | undefined
  token?: string | undefined
  state: ConnectionState
  hub: Hub
  lastMessageAt: number
}

type HttpResponse = {
  statusCode: number
  body: string
}

/**
 * Client protocol sent by Microsoft's own JavaScript and .NET clients. Servers
 * reply with the version they speak, anything from 1.4 to 2.1 is accepted.
 */
const CLIENT_PROTOCOL = '2.1'
const SUPPORTED_PROTOCOLS = ['1.4', '1.5', '2.0', '2.1']
/** Protocols whose negotiate response can be a redirect. */
const REDIRECT_PROTOCOLS = ['2.0', '2.1']
const MAX_REDIRECTS = 100
/** Warn about a slow connection past this share of the keep-alive timeout. */
const KEEP_ALIVE_WARN_AT = 2 / 3
/** Errors that retrying can't fix: the connection is stopped. */
const FATAL_CODES = new Set<ErrorCode>([
  'UNAUTHORIZED',
  'ERR_PROTOCOL',
  'NO_TRANSPORT',
  'UNSUPPORTED_WEBSOCKET'
])

function qsString(pairs: [string, string][]): string {
  return pairs
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join('&')
}

export interface Client {
  on<E extends keyof ClientEvents>(event: E, listener: ClientEvents[E]): this
  once<E extends keyof ClientEvents>(event: E, listener: ClientEvents[E]): this
  off<E extends keyof ClientEvents>(event: E, listener: ClientEvents[E]): this
  emit<E extends keyof ClientEvents>(
    event: E,
    ...args: Parameters<ClientEvents[E]>
  ): boolean
}

/**
 * A signalR client for Node.js which support ASP.net but not ASP.net Core.
 * For ASP.net Core signalR support use the offical client from Microsoft.
 *
 * Modelled after Microsoft's JavaScript and .NET clients for ASP.NET SignalR
 * (client protocol 2.1): WebSockets, server-sent events and long polling,
 * with the same fallback, reconnect and keep-alive behavior.
 */
export class Client extends EventEmitter {
  readonly subscribedHubs: { name: string }[] = []

  /** Custom query string parameters added to every request. */
  qs: Record<string, string> = {}
  headers: Record<string, string> = {}
  /**
   * Cookie jar shared by every request, WebSocket handshake included.
   * Pre-populate it to authenticate with existing cookies.
   */
  cookieJar = new CookieJar()
  /**
   * Base undici dispatcher (e.g. `Agent`, `ProxyAgent`). Cookie handling is
   * layered on top of it automatically.
   */
  dispatcher: Dispatcher | undefined = undefined
  /**
   * Transports to use, in order of preference. When one fails to connect the
   * next one is tried. `webSockets` is skipped if the server doesn't support
   * it.
   */
  transports: TransportName[] = ['webSockets', 'serverSentEvents', 'longPolling']

  requestTimeout = 5000
  /** Delay (ms) before a reconnect attempt. */
  reconnectDelayTime = 2000
  /** Wait (ms) before long polling polls again after a failed poll. */
  longPollReconnectDelay = 3000
  callTimeout = 5000
  /**
   * Interval (ms) of the `/ping` requests that keep the server session alive.
   * 0 disables them.
   */
  pingInterval = 300_000
  /**
   * Extra time (ms) added to the server's `TransportConnectTimeout`, the time
   * a transport has to deliver the init message.
   */
  transportConnectTimeout = 0

  connection: Connection = {
    state: 'Disconnected',
    hub: new Hub(this),
    lastMessageAt: Date.now()
  }

  _callTimeout = 0

  private invocationId = 0

  // Endpoint, updated by the negotiate response and redirects.
  private baseUrl = ''
  private appRelativeUrl = ''
  private webSocketServerUrl: string | undefined
  private redirectQs: string | undefined
  private accessToken: string | undefined

  // Negotiated settings.
  private disconnectTimeout = 30_000
  private pollTimeout = 120_000
  private totalTransportConnectTimeout = 0
  private keepAliveTimeout = 0
  private keepAliveWarning = 0
  private beatInterval = 5000

  private httpDispatcher!: Dispatcher
  private active = false
  private runId = 0
  private runAbort: AbortController | undefined

  private transport: Transport | undefined
  private activeTransports: TransportName[] = []
  private transportIndex = 0
  private initialized = false
  private started = false
  private resuming = false
  private messageId: string | undefined
  private groupsToken: string | undefined
  private buffer: Message[] = []

  private connectionLostAt: number | null = null
  private keepAliveNotified = false
  private initTimer: NodeJS.Timeout | null = null
  private beatTimer: NodeJS.Timeout | null = null
  private pingTimer: NodeJS.Timeout | null = null
  private reconnectCount = 0
  private reconnectTimer: NodeJS.Timeout | null = null

  constructor(
    public readonly url: string,
    hubs: string[]
  ) {
    super()

    if (hubs && hubs.length > 0) {
      this.subscribedHubs = hubs.map((hubName) => ({
        name: hubName.toLocaleLowerCase()
      }))
    }
  }

  // ─── URLs ───────────────────────────────────────────────────────────────

  private _setUrl(url: string): void {
    const parsed = new URL(url)
    this.baseUrl = parsed.origin
    this.appRelativeUrl = parsed.pathname.replace(/\/+$/, '')
  }

  private get transportName(): TransportName {
    /* istanbul ignore next -- negotiation fails without a transport */
    return this.activeTransports[this.transportIndex] ?? 'webSockets'
  }

  private _headers(): Record<string, string> {
    return this.accessToken
      ? { ...this.headers, Authorization: `Bearer ${this.accessToken}` }
      : { ...this.headers }
  }

  /** `clientProtocol`, query string, token and hubs of every request. */
  private _commonPairs(): [string, string][] {
    const pairs: [string, string][] = [['clientProtocol', CLIENT_PROTOCOL]]
    if (this.redirectQs !== undefined) {
      pairs.push(...new URLSearchParams(this.redirectQs))
    } else {
      pairs.push(...Object.entries(this.qs))
    }
    if (this.connection.token) {
      pairs.push(['connectionToken', this.connection.token])
    }
    pairs.push(['connectionData', JSON.stringify(this.subscribedHubs)])
    return pairs
  }

  private _endpoint(path: string): string {
    return `${this.baseUrl}${this.appRelativeUrl}/${path}`
  }

  /** `negotiate`, `start`, `send` and `abort` URLs. */
  private _ajaxUrl(path: string, transport?: TransportName): string {
    const pairs: [string, string][] = transport
      ? [['transport', transport], ...this._commonPairs()]
      : this._commonPairs()
    return `${this._endpoint(path)}?${qsString(pairs)}`
  }

  private _connectRequest(
    endpoint: ConnectEndpoint,
    transport: TransportName,
    post: boolean
  ): ConnectRequest {
    const pairs: [string, string][] = [['transport', transport]]
    if (!post && this.groupsToken) {
      pairs.push(['groupsToken', this.groupsToken])
    }
    if (!post && endpoint !== 'connect' && this.messageId) {
      pairs.push(['messageId', this.messageId])
    }
    pairs.push(...this._commonPairs())
    if (this.accessToken && transport !== 'longPolling') {
      // Browsers can't set headers on these, so the service takes the token
      // from the query string.
      pairs.push(['access_token', this.accessToken])
    }
    if (!post) pairs.push(['tid', `${Math.floor(Math.random() * 11)}`])

    const base =
      transport === 'webSockets'
        ? (this.webSocketServerUrl?.replace(/\/+$/, '') ??
          this.baseUrl.replace(/^http/, 'ws'))
        : this.baseUrl

    let body: string | undefined
    if (post) {
      const form = new URLSearchParams()
      if (this.messageId) form.set('messageId', this.messageId)
      if (this.groupsToken) form.set('groupsToken', this.groupsToken)
      body = form.toString()
    }

    return {
      url: `${base}${this.appRelativeUrl}/${endpoint}?${qsString(pairs)}`,
      body
    }
  }

  // ─── HTTP ───────────────────────────────────────────────────────────────

  private async _http(
    method: 'GET' | 'POST',
    url: string,
    code: ErrorCode,
    signal?: AbortSignal
  ): Promise<HttpResponse> {
    const timeout = this.requestTimeout || 5000
    // jQuery adds a cache-buster to every GET it makes with `cache: false`.
    if (method === 'GET') {
      url += `${url.includes('?') ? '&' : '?'}_=${Date.now()}`
    }
    const timeoutSignal = AbortSignal.timeout(timeout)

    try {
      const res = await request(url, {
        method,
        headers:
          method === 'POST'
            ? { ...this._headers(), 'Content-Type': FORM_CONTENT_TYPE }
            : this._headers(),
        dispatcher: this.httpDispatcher,
        signal: signal ? AbortSignal.any([signal, timeoutSignal]) : timeoutSignal
      })
      const body = await res.body.text()

      return { statusCode: res.statusCode, body }
    } catch (e) {
      if (e instanceof Error && e.name === 'TimeoutError') {
        throw new SignalRError(code, `Timeout of ${timeout}ms exceeded.`)
      }
      throw SignalRError.from(code, e as Error)
    }
  }

  private _statusError(statusCode: number, code: ErrorCode): SignalRError {
    if (statusCode === 401 || statusCode === 302) {
      return new SignalRError(
        'UNAUTHORIZED',
        `Server responded with status code ${statusCode}, stopping the connection.`
      )
    }
    return new SignalRError(
      code,
      `Server responded with status code ${statusCode}.`
    )
  }

  private async _negotiate(signal: AbortSignal): Promise<NegotiateProtocol> {
    for (let redirects = 0; ; redirects++) {
      const res = await this._http(
        'GET',
        this._ajaxUrl('negotiate'),
        'ERR_NEGOTIATE',
        signal
      )

      if (res.statusCode !== 200) {
        throw this._statusError(res.statusCode, 'ERR_NEGOTIATE')
      }

      let protocol: NegotiateProtocol
      try {
        protocol = JSON.parse(res.body)
      } catch {
        throw new SignalRError(
          'ERR_NEGOTIATE',
          'Error parsing negotiate response.'
        )
      }

      if (protocol.availableTransports) {
        throw new SignalRError(
          'ERR_PROTOCOL',
          'The server is an ASP.NET Core SignalR server. Use the official ' +
            'client from Microsoft for ASP.NET Core SignalR instead.'
        )
      }

      const version = protocol.ProtocolVersion
      if (!version || !SUPPORTED_PROTOCOLS.includes(version)) {
        throw new SignalRError(
          'ERR_PROTOCOL',
          `You are using a version of the client that isn't compatible with the server. Client version ${CLIENT_PROTOCOL}, server version ${version}.`
        )
      }

      if (REDIRECT_PROTOCOLS.includes(version)) {
        if (protocol.Error) {
          throw new SignalRError(
            'ERR_PROTOCOL',
            `Error message received from the server: '${protocol.Error}'.`
          )
        }
        if (protocol.RedirectUrl) {
          if (redirects === MAX_REDIRECTS) {
            throw new SignalRError(
              'ERR_NEGOTIATE',
              'Negotiate redirection limit exceeded.'
            )
          }
          this.accessToken = protocol.AccessToken
          const [target, query] = protocol.RedirectUrl.split('?', 2) as [
            string,
            string?
          ]
          this._setUrl(target)
          // Only the query string of the most recent redirect applies.
          this.redirectQs = query
          continue
        }
      }

      return protocol
    }
  }

  private _applyNegotiation(protocol: NegotiateProtocol): void {
    this.connection.id = protocol.ConnectionId
    this.connection.token = protocol.ConnectionToken
    if (protocol.Url) this.appRelativeUrl = protocol.Url.replace(/\/+$/, '')
    this.webSocketServerUrl = protocol.WebSocketServerUrl || undefined

    this.pollTimeout = (protocol.ConnectionTimeout ?? 110) * 1000 + 10_000
    this.disconnectTimeout = (protocol.DisconnectTimeout ?? 30) * 1000
    this.totalTransportConnectTimeout =
      protocol.TransportConnectTimeout === undefined
        ? 0
        : this.transportConnectTimeout + protocol.TransportConnectTimeout * 1000

    if (protocol.KeepAliveTimeout) {
      this.keepAliveTimeout = protocol.KeepAliveTimeout * 1000
      this.keepAliveWarning = this.keepAliveTimeout * KEEP_ALIVE_WARN_AT
      this.beatInterval = (this.keepAliveTimeout - this.keepAliveWarning) / 3
    } else {
      this.keepAliveTimeout = 0
    }

    this.activeTransports = this.transports.filter(
      (name) => name !== 'webSockets' || protocol.TryWebSockets
    )
    this.transportIndex = 0
    if (this.activeTransports.length === 0) {
      throw new SignalRError(
        this.transports.includes('webSockets') && !protocol.TryWebSockets
          ? 'UNSUPPORTED_WEBSOCKET'
          : 'NO_TRANSPORT',
        this.transports.includes('webSockets') && !protocol.TryWebSockets
          ? 'Websocket is not supported'
          : 'No transports were selected.'
      )
    }
  }

  private async _start(signal: AbortSignal): Promise<void> {
    const res = await this._http(
      'GET',
      this._ajaxUrl('start', this.transportName),
      'ERR_START',
      signal
    )

    if (res.statusCode !== 200) {
      throw this._statusError(res.statusCode, 'ERR_START')
    }

    let data: { Response?: string } | null
    try {
      data = JSON.parse(res.body)
    } catch {
      throw new SignalRError('ERR_START', 'Error parsing start response.')
    }
    if (data?.Response !== 'started') {
      throw new SignalRError(
        'ERR_START',
        `Invalid start response: '${res.body}'.`
      )
    }
  }

  private async _abort(): Promise<void> {
    await this._http('POST', this._ajaxUrl('abort', this.transportName), 'ERR_ABORT')
  }

  private async _ping(): Promise<void> {
    if (this.connection.state !== 'Connected') return

    try {
      const query = qsString(Object.entries(this.qs))
      const res = await this._http(
        'GET',
        `${this._endpoint('ping')}${query ? `?${query}` : ''}`,
        'ERR_PING'
      )

      if (res.statusCode === 401 || res.statusCode === 403) {
        this._error(
          new SignalRError(
            'UNAUTHORIZED',
            `Ping failed with status code ${res.statusCode}.`
          )
        )
        this._stop('unauthorized', false)
        return
      }
      if (res.statusCode !== 200) {
        throw new SignalRError(
          'ERR_PING',
          `Ping failed with status code ${res.statusCode}.`
        )
      }
      let data: { Response?: string } | null = null
      try {
        data = JSON.parse(res.body)
      } catch {
        // handled below
      }
      if (data?.Response !== 'pong') {
        throw new SignalRError(
          'ERR_PING',
          `Invalid ping response: '${res.body}'.`
        )
      }
    } catch (e) {
      this._error(
        /* istanbul ignore next -- _http only throws SignalRErrors */
        e instanceof SignalRError ? e : SignalRError.from('ERR_PING', e as Error)
      )
    }
  }

  // ─── Connection lifecycle ───────────────────────────────────────────────

  start(): void {
    if (this.active) return

    if (!this.url) {
      this._error(new SignalRError('INVALID_URL', 'Invalid URL.'))
      return
    }
    try {
      const parsed = new URL(this.url)
      if (parsed.protocol !== 'http:' && parsed.protocol !== 'https:') {
        this._error(new SignalRError('INVALID_PROTOCOL', 'Invalid protocol.'))
        return
      }
    } catch {
      this._error(new SignalRError('INVALID_URL', 'Invalid URL.'))
      return
    }
    if (this.subscribedHubs.length === 0) {
      this._error(
        new SignalRError('NO_HUB', 'No hubs have been subscribed to.')
      )
      return
    }

    this.httpDispatcher = (this.dispatcher ?? new Agent()).compose(
      cookie({ cookies: { jar: this.cookieJar } })
    )
    this.active = true
    this.connection.state = 'Connecting'
    this._run()
  }

  /** Negotiate a new connection and start connecting it. */
  private _run(): void {
    const runId = ++this.runId
    this.runAbort?.abort()
    const abort = (this.runAbort = new AbortController())

    this.started = false
    this.initialized = false
    this.resuming = false
    this.messageId = undefined
    this.groupsToken = undefined
    this.accessToken = undefined
    this.redirectQs = undefined
    this.connection.id = undefined
    this.connection.token = undefined
    this._setUrl(this.url)

    this._negotiate(abort.signal)
      .then((protocol) => {
        if (runId !== this.runId) return
        this._applyNegotiation(protocol)
        this._connect()
      })
      .catch((error: unknown) => {
        if (runId !== this.runId) return
        this._startFailed(error)
      })
  }

  /** Negotiating or starting failed. */
  private _startFailed(error: unknown): void {
    const _error =
      error instanceof SignalRError
        ? error
        : SignalRError.from('ERR_NEGOTIATE', error as Error)
    this._error(_error)

    if (FATAL_CODES.has(_error.code)) {
      this._stop(_error.code === 'UNAUTHORIZED' ? 'unauthorized' : 'failed', false)
    } else {
      this._reconnect(true)
    }
  }

  private _host(owner: () => Transport | undefined): TransportHost {
    const client = this
    return {
      dispatcher: this.httpDispatcher,
      get headers() {
        return client._headers()
      },
      get requestTimeout() {
        return client.requestTimeout || 5000
      },
      get pollTimeout() {
        return client.pollTimeout
      },
      get longPollReconnectDelay() {
        return client.longPollReconnectDelay
      },
      get connecting() {
        return !client.resuming && !client.initialized
      },
      canRetry: () =>
        (client.connection.state === 'Connected' ||
          client.connection.state === 'Reconnecting') &&
        Date.now() - client.connection.lastMessageAt <= client.disconnectTimeout,
      onError: (error) => {
        if (client.connection.state !== 'Reconnecting') {
          client._error(SignalRError.from('ERR_SOCKET', error))
        }
      },
      get reconnectAttempt() {
        return client.reconnectCount
      },
      request: (endpoint, transport, post) =>
        client._connectRequest(endpoint, transport, post),
      sendUrl: (transport) => client._ajaxUrl('send', transport),
      onOpen: () => client._onTransportOpen(owner()),
      onData: (raw) => client._receiveMessage(raw),
      onActivity: () => client._markLastMessage(),
      onClose: (error) => client._onTransportClosed(owner(), error),
      onReconnecting: () => client._onTransportReconnecting(owner()),
      onReconnected: () => client._onTransportReconnected(owner())
    }
  }

  private _connect(): void {
    const name = this.transportName
    let transport: Transport | undefined
    const host = this._host(() => transport)
    transport =
      name === 'webSockets'
        ? new WebSocketTransport(host)
        : name === 'serverSentEvents'
          ? new ServerSentEventsTransport(host)
          : new LongPollingTransport(host)

    this.transport = transport

    // The transport has this long, from now, to deliver the init message.
    this._clearInitTimer()
    if (!this.resuming && this.totalTransportConnectTimeout > 0) {
      const timer = (this.initTimer = setTimeout(() => {
        this.initTimer = null
        const current = transport
        current?.close()
        this._onTransportClosed(current, new Error('Transport connect timeout.'))
      }, this.totalTransportConnectTimeout))
      timer.unref()
    }

    transport.connect(this.resuming)
  }

  private _onTransportOpen(transport: Transport | undefined): void {
    if (!transport || transport !== this.transport) return
    // A reconnect has no init message nor start request.
    if (this.resuming) this._onConnected()
  }

  private _onTransportReconnecting(transport: Transport | undefined): void {
    if (!transport || transport !== this.transport) return
    if (this.connection.state === 'Connected') {
      this.connection.state = 'Reconnecting'
      this.emit('reconnecting', ++this.reconnectCount)
    }
  }

  private _onTransportReconnected(transport: Transport | undefined): void {
    if (!transport || transport !== this.transport) return
    if (this.connection.state === 'Reconnecting') this._onConnected()
  }

  private _onTransportClosed(
    transport: Transport | undefined,
    error?: Error
  ): void {
    if (!transport || transport !== this.transport) return
    this.transport = undefined
    this._clearInitTimer()

    if (!this.resuming && !this.initialized) {
      // The transport never got going: fall back to the next one.
      if (this.transportIndex + 1 < this.activeTransports.length) {
        ++this.transportIndex
        this._connect()
        return
      }
      this._error(
        new SignalRError(
          'ERR_CONNECT',
          `Fallback transports exhausted${error ? `: ${error.message}` : '.'}`
        )
      )
      this._reconnect(true)
      return
    }

    if (!this.resuming && !this.started) {
      // Lost during the start request: begin again.
      this._error(
        new SignalRError(
          'ERR_START',
          `${transport.name} transport failed during the start request.`
        )
      )
      this._reconnect(true)
      return
    }

    this._lose(error ? SignalRError.from('ERR_SOCKET', error) : undefined)
  }

  /** Init message received: the start request comes next. */
  private _onInit(): void {
    // An init message after a reconnect is ignored.
    if (this.resuming || this.initialized) return
    this.initialized = true
    this._clearInitTimer()

    const runId = this.runId
    /* istanbul ignore next -- a run always has an abort controller */
    const signal = this.runAbort?.signal ?? new AbortController().signal
    this._start(signal)
      .then(() => {
        if (runId === this.runId) this._onConnected()
      })
      .catch((error: unknown) => {
        if (runId === this.runId) this._startFailed(error)
      })
  }

  private _onConnected(): void {
    this.reconnectCount = 0
    this.connectionLostAt = null
    this._callTimeout = 0
    this.started = true
    this.connection.state = 'Connected'
    this._markLastMessage()
    this._startHeartbeat()
    this._startPing()

    // Messages that arrived while connecting.
    const buffered = this.buffer
    this.buffer = []
    for (const message of buffered) this._dispatchInvocation(message)

    this.emit('connected')
  }

  /** The connection was lost: try to resume it. */
  private _lose(error?: SignalRError): void {
    if (error) this._error(error)
    this._callTimeout = 1000
    this._markConnectionLost()
    this.connection.state = 'Disconnected'
    if (this.transportName === 'webSockets') {
      // Results can't be delivered after the socket is gone.
      this.connection.hub._clearCallbacks(
        'Connection started reconnecting before invocation result was received.'
      )
    }
    this.emit('disconnected', 'failed')
    this._reconnect()
  }

  private _reconnect(restart = false): void {
    if (this.reconnectTimer) return
    this._clearBeatTimer()
    this._clearInitTimer()
    this._markConnectionLost()
    this._closeTransport()
    this.reconnectTimer = setTimeout(() => {
      this.reconnectTimer = null
      ++this.reconnectCount
      this.connection.state = 'Reconnecting'
      this.emit('reconnecting', this.reconnectCount)
      // Past the server's DisconnectTimeout the connection can't be resumed,
      // so negotiate a new one.
      const expired =
        this.connectionLostAt !== null &&
        Date.now() - this.connectionLostAt > this.disconnectTimeout
      if (restart || expired || this.messageId === undefined) {
        this._run()
      } else {
        this.resuming = true
        this._connect()
      }
    }, this.reconnectDelayTime || 2000)
  }

  private _closeTransport(): void {
    if (this.transport) {
      this.transport.close()
      this.transport = undefined
    }
  }

  private _clearReconnectTimer(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
  }

  private _clearInitTimer(): void {
    if (this.initTimer) {
      clearTimeout(this.initTimer)
      this.initTimer = null
    }
  }

  // ─── Keep-alive and ping ────────────────────────────────────────────────

  private _startHeartbeat(): void {
    this._clearBeatTimer()
    this.keepAliveNotified = false
    // Only transports on which the server sends keep-alives are monitored.
    if (!this.keepAliveTimeout || !this.transport?.supportsKeepAlive) return
    this.beatTimer = setTimeout(() => this._beat(), this.beatInterval)
  }

  private _beat(): void {
    this.beatTimer = null

    /* istanbul ignore else -- the beat stops with the connection */
    if (this.connection.state === 'Connected') {
      const elapsed = Date.now() - this.connection.lastMessageAt
      if (elapsed >= this.keepAliveTimeout) {
        this._lose(
          new SignalRError(
            'CONNECTION_LOST',
            'Keep alive timed out. Connection has been lost.'
          )
        )
        return
      }
      if (elapsed >= this.keepAliveWarning) {
        if (!this.keepAliveNotified) {
          this.keepAliveNotified = true
          this.emit('connectionSlow')
        }
      } else {
        this.keepAliveNotified = false
      }
    }

    this.beatTimer = setTimeout(() => this._beat(), this.beatInterval)
  }

  private _clearBeatTimer(): void {
    if (this.beatTimer) {
      clearTimeout(this.beatTimer)
      this.beatTimer = null
    }
  }

  private _startPing(): void {
    if (this.pingTimer || !this.pingInterval) return
    this.pingTimer = setInterval(() => void this._ping(), this.pingInterval)
    this.pingTimer.unref()
  }

  private _markLastMessage(): void {
    this.connection.lastMessageAt = Date.now()
  }

  private _markConnectionLost(): void {
    if (this.connectionLostAt === null) this.connectionLostAt = Date.now()
  }

  // ─── Messages ───────────────────────────────────────────────────────────

  private _receiveMessage(raw: string): DataResult | undefined {
    if (!raw) return undefined

    let data: SignalRMessage | null
    try {
      data = JSON.parse(raw)
    } catch (e) {
      this._parseFailed(raw, e as Error)
      return undefined
    }
    if (data === null || typeof data !== 'object') return undefined

    return this._processMessage(data)
  }

  private _parseFailed(raw: string, error: Error): void {
    const failure = SignalRError.from(
      'ERR_PARSE',
      new Error(`Failed at parsing response: ${raw} (${error.message})`)
    )
    if (!this.resuming && !this.initialized) {
      // Still connecting: this transport is not usable.
      const transport = this.transport
      transport?.close()
      this._onTransportClosed(transport, failure)
      return
    }
    this._error(failure)
    this._stop('failed', false)
  }

  private _processMessage(data: SignalRMessage): DataResult | undefined {
    if (data.I !== undefined || data.P !== undefined) {
      // The result of an invocation.
      this.connection.hub._handleResult(data)
      return undefined
    }

    this._markLastMessage()

    if (data.E) {
      // A global error: the server wants the connection to stop.
      this._error(new SignalRError('ERR_SERVER', data.E))
      this._stop('failed', false)
      return undefined
    }

    if (data.G) this.groupsToken = data.G
    if (data.C) this.messageId = data.C

    if (data.M) {
      for (const message of data.M) this._deliver(message)
      if (data.S !== undefined) this._onInit()
    }

    const result: DataResult = {}
    if (data.T !== undefined) result.shouldReconnect = true
    if (typeof data.L === 'number') result.delay = data.L
    return result
  }

  private _deliver(message: Message): void {
    if (this.connection.state === 'Connecting') {
      this.buffer.push(message)
      return
    }
    this._dispatchInvocation(message)
  }

  private _dispatchInvocation(message: Message): void {
    const hubName = message.H.toLowerCase()
    const hub = this.connection.hub
    hub._updateState(hubName, message.S)
    const method = hub.handlers[hubName]?.[message.M.toLowerCase()]
    if (!method) return
    try {
      method.apply(this, message.A)
    } catch (e) {
      // An error in a handler must not take the transport down.
      process.nextTick(() => {
        throw e
      })
    }
  }

  /** @internal */
  _nextInvocationId(): number {
    return this.invocationId++
  }

  /** @internal */
  _sendMessage(
    hub: string,
    method: string,
    args: unknown[],
    id: number
  ): void {
    const state = this.connection.hub.state[hub.toLowerCase()]
    const payload = JSON.stringify({
      H: hub.toLowerCase(),
      M: method,
      A: args,
      I: id,
      ...(state && Object.keys(state).length > 0 ? { S: state } : {})
    })

    const fail = (error: SignalRError): void => {
      if (!this.connection.hub._handleSendError(id, error)) this._error(error)
    }

    const transport = this.transport
    const state_ = this.connection.state
    if (
      !transport ||
      !transport.ready ||
      (state_ !== 'Connected' && state_ !== 'Reconnecting')
    ) {
      fail(
        new SignalRError(
          'ERR_SEND',
          'The connection must be started before data can be sent.'
        )
      )
      return
    }

    transport
      .send(payload)
      .then((response) => {
        // Server-sent events and long polling deliver the result of an
        // invocation in the response of the send request.
        if (response) this._receiveSendResponse(response, id)
      })
      .catch((error: Error) => {
        fail(SignalRError.from('ERR_SEND', error))
      })
  }

  private _receiveSendResponse(
    raw: string,
    id: number
  ): void {
    let data: SignalRMessage | null
    try {
      data = JSON.parse(raw)
    } catch (e) {
      // An unreadable response means the connection can't be trusted: the
      // reference client reports the error and stops.
      const error = SignalRError.from('ERR_PARSE', e as Error)
      this.connection.hub._handleSendError(id, error)
      this._error(error)
      this._stop('failed', true)
      return
    }
    if (data && (data.I !== undefined || data.P !== undefined)) {
      this.connection.hub._handleResult(data)
    }
  }

  private _error(error: SignalRError): void {
    // `error` is special on an EventEmitter: it throws without a listener.
    if (this.listenerCount('error') > 0) this.emit('error', error)
  }

  // ─── Stopping ───────────────────────────────────────────────────────────

  private _stop(reason: 'failed' | 'unauthorized' | 'end', notify: boolean): void {
    if (!this.active) return

    // Tell the server before the connection token is dropped.
    if (notify && this.started && this.connection.token) {
      this._abort().catch(() => {})
    }

    this.active = false
    this.runId++
    this.runAbort?.abort()
    this.runAbort = undefined
    this._clearReconnectTimer()
    this._clearBeatTimer()
    this._clearInitTimer()
    if (this.pingTimer) {
      clearInterval(this.pingTimer)
      this.pingTimer = null
    }
    this._closeTransport()

    this.connection.hub._clearCallbacks(
      'Connection was disconnected before invocation result was received.'
    )
    this.buffer = []
    this.started = false
    this.initialized = false
    this.resuming = false
    this.reconnectCount = 0
    this.connectionLostAt = null
    this._callTimeout = 0
    this.messageId = undefined
    this.groupsToken = undefined
    this.accessToken = undefined
    this.redirectQs = undefined
    this.connection.id = undefined
    this.connection.token = undefined
    this.connection.state = 'Disconnected'

    this.emit('disconnected', reason)
  }

  end(): void {
    this._stop('end', true)
  }
}
