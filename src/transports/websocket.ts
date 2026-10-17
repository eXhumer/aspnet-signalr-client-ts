import { WebSocket } from 'undici'
import type { Transport, TransportHost } from './types.js'

export class WebSocketTransport implements Transport {
  readonly name = 'webSockets'
  readonly supportsKeepAlive = true

  #socket: WebSocket | undefined
  #error: Error | undefined
  #handshakeTimer: NodeJS.Timeout | undefined

  constructor(private readonly host: TransportHost) {}

  get ready(): boolean {
    return this.#socket?.readyState === WebSocket.OPEN
  }

  connect(resume: boolean): void {
    const host = this.host
    const { url } = host.request(
      resume ? 'reconnect' : 'connect',
      this.name,
      false
    )
    const socket = new WebSocket(url, {
      dispatcher: host.dispatcher,
      headers: host.headers
    })
    this.#socket = socket

    const handshakeTimer = (this.#handshakeTimer = setTimeout(
      () => socket.close(),
      host.requestTimeout
    ))
    handshakeTimer.unref()

    socket.onopen = (): void => {
      clearTimeout(handshakeTimer)
      host.onOpen()
    }
    socket.onerror = (event): void => {
      this.#error = new Error(event.message)
    }
    socket.onmessage = (event): void => {
      // Only text frames carry SignalR payloads.
      if (typeof event.data === 'string') host.onData(event.data)
    }
    socket.onclose = (): void => {
      clearTimeout(handshakeTimer)
      host.onClose(this.#error)
    }
  }

  async send(payload: string): Promise<undefined> {
    if (!this.ready) throw new Error('WebSocket is not open.')
    this.#socket!.send(payload)
    return undefined
  }

  close(): void {
    clearTimeout(this.#handshakeTimer)
    const socket = this.#socket
    this.#socket = undefined
    if (socket) {
      socket.onopen = null
      socket.onclose = null
      socket.onmessage = null
      socket.onerror = null
      socket.close()
    }
  }
}
