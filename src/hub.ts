import { HubError, SignalRError } from './errors.js'
import type { SignalRMessage } from './messages.js'
import type { Client } from './index.js'

export type HubEvent = (...args: any[]) => void

type PendingCall = {
  hub: string
  resolve: (result: unknown) => void
  reject: (error: unknown) => void
  timer: NodeJS.Timeout
}

type HubHandler = {
  [key: string]: { [key: string]: HubEvent }
}

export class Hub {
  private callbacks = new Map<number, PendingCall>()

  handlers: HubHandler = {}
  /**
   * State shared with the server, per (lower cased) hub name. It is sent with
   * every invocation and updated from server messages and results.
   */
  state: Record<string, Record<string, unknown>> = {}

  constructor(private client: Client) {}

  /** @internal */
  _updateState(hub: string, state: Record<string, unknown> | undefined): void {
    if (state && typeof state === 'object') {
      this.state[hub] = { ...this.state[hub], ...state }
    }
  }

  /** @internal Handle the result (or progress) of an invocation. */
  _handleResult(data: SignalRMessage): void {
    // Progress updates carry the invocation id inside P. The invocation stays
    // pending until its result arrives.
    if (data.P !== undefined) return

    const id = Number(data.I)
    const pending = this.callbacks.get(id)
    if (!pending) return

    clearTimeout(pending.timer)
    this.callbacks.delete(id)
    this._updateState(
      pending.hub,
      data.S && typeof data.S === 'object' ? data.S : undefined
    )

    if (data.E) {
      pending.reject(new HubError(data.E, data.H === true, data.D))
    } else {
      pending.resolve(data.R)
    }
  }

  /** @internal The invocation couldn't be sent. */
  _handleSendError(id: number, error: SignalRError): boolean {
    const pending = this.callbacks.get(id)
    if (!pending) return false
    clearTimeout(pending.timer)
    this.callbacks.delete(id)
    pending.reject(error)
    return true
  }

  /** @internal Reject every pending invocation. */
  _clearCallbacks(message: string): void {
    for (const [id, pending] of this.callbacks) {
      clearTimeout(pending.timer)
      this.callbacks.delete(id)
      pending.reject(new SignalRError('ERR_ABORT', message))
    }
  }

  /**
   * Bind events to receive messages.
   */
  on(hubName: string, methodName: string, cb: HubEvent): void {
    const _hubName = hubName.toLowerCase()
    let handler = this.handlers[_hubName]
    if (!handler) {
      handler = this.handlers[_hubName] = {}
    }
    handler[methodName.toLowerCase()] = cb
  }

  private _args(args: unknown[]): unknown[] {
    return args.map((arg) =>
      typeof arg === 'function' || typeof arg === 'undefined' ? null : arg
    )
  }

  /**
   * Call the hub method and get return values asynchronously
   */
  call(hubName: string, methodName: string, ...args: unknown[]): Promise<unknown> {
    return new Promise((resolve, reject) => {
      const id = this.client._nextInvocationId()
      const timer = setTimeout(
        () => {
          this.callbacks.delete(id)
          reject('Timeout')
        },
        this.client._callTimeout || this.client.callTimeout || 5000
      )
      this.callbacks.set(id, {
        hub: hubName.toLowerCase(),
        resolve,
        reject,
        timer
      })
      this.client._sendMessage(hubName, methodName, this._args(args), id)
    })
  }

  /**
   * Invoke the hub method without return values
   */
  invoke(hubName: string, methodName: string, ...args: unknown[]): void {
    this.client._sendMessage(
      hubName,
      methodName,
      this._args(args),
      this.client._nextInvocationId()
    )
  }
}
