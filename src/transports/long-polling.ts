import { request } from 'undici'
import { FORM_CONTENT_TYPE, HttpStatusError, postSend } from './shared.js'
import type { ConnectEndpoint, Transport, TransportHost } from './types.js'

/** 1 hour, the cap of the "reconnected" heuristic. */
const MAX_REOPEN_DELAY = 3_600_000

/**
 * Long polling: `/connect` (or `/reconnect`) is the first poll, `/poll` keeps
 * polling afterwards. Polls are `POST` requests; the message id and groups
 * token travel in the body.
 */
export class LongPollingTransport implements Transport {
  readonly name = 'longPolling'
  // The server sends no keep-alives to long polling clients.
  readonly supportsKeepAlive = false

  #abort: AbortController | undefined
  #closed = false
  #open = false
  #reopenTimer: NodeJS.Timeout | undefined
  /** `true` while a timer will raise "reconnected" after a failed poll. */
  #reconnectedPending = false
  #wake: (() => void) | undefined
  #wakeTimer: NodeJS.Timeout | undefined

  constructor(private readonly host: TransportHost) {}

  get ready(): boolean {
    return this.#open && !this.#closed
  }

  connect(resume: boolean): void {
    void this.#loop(resume)
  }

  send(payload: string): Promise<string> {
    return postSend(this.host, this.name, payload)
  }

  close(): void {
    this.#closed = true
    clearTimeout(this.#reopenTimer)
    this.#abort?.abort()
    this.#wake?.()
  }

  #sleep(ms: number): Promise<void> {
    return new Promise<void>((resolve) => {
      this.#wake = resolve
      this.#wakeTimer = setTimeout(resolve, ms)
    }).finally(() => {
      clearTimeout(this.#wakeTimer)
      this.#wake = undefined
    })
  }

  #fail(error?: Error): void {
    this.#closed = true
    clearTimeout(this.#reopenTimer)
    this.#abort?.abort()
    this.host.onClose(error)
  }

  #raiseReconnected(): void {
    clearTimeout(this.#reopenTimer)
    this.#reconnectedPending = false
    if (this.#open) this.host.onReconnected()
    else this.#markOpen()
  }

  #markOpen(): void {
    clearTimeout(this.#reopenTimer)
    if (this.#open || this.#closed) return
    this.#open = true
    this.host.onOpen()
  }

  async #loop(resume: boolean): Promise<void> {
    const host = this.host
    let endpoint: ConnectEndpoint = resume ? 'reconnect' : 'connect'
    let reconnectErrors = 0

    if (resume) {
      // A reconnect is a poll the server holds until it has data, so
      // "reconnected" is raised after a delay that grows with every failed
      // attempt, or as soon as the poll returns.
      const delay = Math.min(
        1000 * (2 ** host.reconnectAttempt - 1),
        MAX_REOPEN_DELAY
      )
      this.#reopenTimer = setTimeout(() => this.#markOpen(), delay)
    }

    while (!this.#closed) {
      const abort = (this.#abort = new AbortController())
      const timer = setTimeout(() => abort.abort(), host.pollTimeout)
      timer.unref()

      let text: string
      try {
        const { url, body } = host.request(endpoint, this.name, true)
        const res = await request(url, {
          method: 'POST',
          headers: { ...host.headers, 'Content-Type': FORM_CONTENT_TYPE },
          body: body ?? '',
          dispatcher: host.dispatcher,
          signal: abort.signal
        })
        text = await res.body.text()
        if (res.statusCode !== 200) throw new HttpStatusError(res.statusCode)
      } catch (e) {
        clearTimeout(timer)
        if (this.#closed) return
        // Before the connection is up the transport simply fails so the
        // next one can be tried.
        if (host.connecting || !host.canRetry()) return this.#fail(e as Error)

        // Otherwise it retries by itself: every error is a reconnect error.
        clearTimeout(this.#reopenTimer)
        this.#reconnectedPending = false
        reconnectErrors++
        host.onError(e as Error)
        host.onReconnecting()
        endpoint = 'reconnect'
        await this.#sleep(host.longPollReconnectDelay)
        if (this.#closed) return

        // "Reconnected" is raised after a delay that grows with every
        // failed attempt, or as soon as a poll returns.
        this.#reconnectedPending = true
        this.#reopenTimer = setTimeout(
          () => this.#raiseReconnected(),
          Math.min(1000 * (2 ** reconnectErrors - 1), MAX_REOPEN_DELAY)
        )
        continue
      }
      clearTimeout(timer)
      /* istanbul ignore next -- close() aborts the request, which throws */
      if (this.#closed) return

      // Reset so the next outage raises "reconnected" quickly.
      reconnectErrors = 0
      if (this.#reconnectedPending) this.#raiseReconnected()
      else this.#markOpen()
      if (this.#closed) return
      host.onActivity()

      const result = text ? host.onData(text) : undefined
      if (this.#closed) return

      if (result?.shouldReconnect) {
        // The next request is a reconnect and "reconnected" is raised right
        // away.
        endpoint = 'reconnect'
        host.onReconnecting()
        setTimeout(() => {
          if (!this.#closed) host.onReconnected()
        }, 0)
      } else {
        endpoint = 'poll'
      }

      if (result?.delay && result.delay > 0) {
        await new Promise<void>((resolve) => setTimeout(resolve, result.delay))
      }
    }
  }
}
