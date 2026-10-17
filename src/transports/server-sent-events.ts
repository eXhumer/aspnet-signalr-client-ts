import { request } from 'undici'
import { postSend, HttpStatusError } from './shared.js'
import type { Transport, TransportHost } from './types.js'

/** A reconnecting event source that doesn't open in time is retried. */
const RECONNECT_OPEN_TIMEOUT = 3000

/**
 * Server-sent events: a long lived `GET` stream for server-to-client
 * messages and `POST /send` for client-to-server messages.
 */
export class ServerSentEventsTransport implements Transport {
  readonly name = 'serverSentEvents'
  readonly supportsKeepAlive = true

  #abort: AbortController | undefined
  #closed = false
  #open = false

  constructor(private readonly host: TransportHost) {}

  get ready(): boolean {
    return this.#open && !this.#closed
  }

  connect(resume: boolean): void {
    void this.#run(resume)
  }

  send(payload: string): Promise<string> {
    return postSend(this.host, this.name, payload)
  }

  close(): void {
    this.#closed = true
    this.#abort?.abort()
  }

  #fail(error?: Error): void {
    if (this.#closed) return
    this.#closed = true
    this.#abort?.abort()
    this.host.onClose(error)
  }

  async #run(resume: boolean): Promise<void> {
    const host = this.host
    const abort = (this.#abort = new AbortController())
    const { url } = host.request(
      resume ? 'reconnect' : 'connect',
      this.name,
      false
    )

    let res: Awaited<ReturnType<typeof request>>
    try {
      res = await request(url, {
        method: 'GET',
        headers: { ...host.headers, Accept: 'text/event-stream' },
        dispatcher: host.dispatcher,
        signal: abort.signal,
        headersTimeout: resume ? RECONNECT_OPEN_TIMEOUT : host.requestTimeout,
        bodyTimeout: 0
      })
    } catch (e) {
      return this.#fail(e as Error)
    }

    /* istanbul ignore next -- close() aborts the request, which throws */
    if (this.#closed) return void res.body.destroy()
    if (res.statusCode !== 200) {
      res.body.destroy()
      return this.#fail(new HttpStatusError(res.statusCode))
    }

    this.#open = true
    host.onOpen()

    const decoder = new TextDecoder()
    let buffer = ''
    try {
      for await (const chunk of res.body) {
        /* istanbul ignore next -- close() aborts the body, ending the loop */
        if (this.#closed) return
        host.onActivity()
        buffer += decoder.decode(chunk as Uint8Array, { stream: true })
        buffer = buffer.replace(/\r\n?/g, '\n')
        let end: number
        while ((end = buffer.indexOf('\n\n')) !== -1) {
          this.#dispatch(buffer.slice(0, end))
          buffer = buffer.slice(end + 2)
          if (this.#closed) return
        }
      }
    } catch (e) {
      return this.#fail(e as Error)
    }
    // The server ended the stream: the client reconnects on its own URL
    // (`/reconnect` with the last message id) instead of the event source's
    // built-in retry.
    this.#fail()
  }

  #dispatch(block: string): void {
    const data = block
      .split('\n')
      .filter((line) => line.startsWith('data:'))
      .map((line) => line.slice(5).replace(/^ /, ''))
      .join('\n')
    // `initialized` is a literal that precedes the first JSON payload.
    if (data && data !== 'initialized') this.host.onData(data)
  }
}
