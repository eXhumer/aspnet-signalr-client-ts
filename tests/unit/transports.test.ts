import net from 'node:net'
import { Agent } from 'undici'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { LongPollingTransport } from '../../src/transports/long-polling.js'
import { ServerSentEventsTransport } from '../../src/transports/server-sent-events.js'
import type {
  Transport,
  TransportHost,
  TransportName
} from '../../src/transports/types.js'
import { WebSocketTransport } from '../../src/transports/websocket.js'
import { createMockServer, type MockServer } from '../helpers/mock-server.js'

let server: MockServer | undefined
let transport: Transport | undefined
const agent = new Agent()

afterEach(async () => {
  transport?.close()
  transport = undefined
  await server?.close()
  server = undefined
})

type Stub = TransportHost & {
  [K in keyof TransportHost as TransportHost[K] extends (...a: never[]) => unknown
    ? K
    : never]: TransportHost[K] & ReturnType<typeof vi.fn>
}

/** A host that talks to the mock server (or to `base`). */
function createHost(
  base: string,
  overrides: Partial<Record<keyof TransportHost, unknown>> = {}
): Stub {
  const host = {
    dispatcher: agent,
    headers: {},
    requestTimeout: 1000,
    pollTimeout: 5000,
    longPollReconnectDelay: 5,
    connecting: false,
    reconnectAttempt: 0,
    request: vi.fn((endpoint: string, name: TransportName) => ({
      url: `${base}/${endpoint}?transport=${name}`,
      body: undefined
    })),
    sendUrl: vi.fn(() => `${base}/send`),
    onOpen: vi.fn(),
    onData: vi.fn(),
    onActivity: vi.fn(),
    onClose: vi.fn(),
    canRetry: vi.fn(() => true),
    onError: vi.fn(),
    onReconnecting: vi.fn(),
    onReconnected: vi.fn(),
    ...overrides
  }
  return host as unknown as Stub
}

const requests = (path: string): MockServer['requests'] =>
  server!.requests.filter((r) => r.path === `/signalr/${path}`)

const tick = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms))

describe('long polling transport', () => {
  it('treats an empty poll response as no payload', async () => {
    server = await createMockServer()
    server.overrides.push({ path: '/signalr/connect', status: 200, body: '' })
    const host = createHost(server.url)
    transport = new LongPollingTransport(host)
    transport.connect(false)
    await vi.waitFor(() => expect(host.onActivity).toHaveBeenCalled())
    expect(host.onOpen).toHaveBeenCalledTimes(1)
    expect(host.onData).not.toHaveBeenCalled()
    expect(transport.ready).toBe(true)
  })

  it('stops when it is closed while opening', async () => {
    server = await createMockServer()
    const host = createHost(server.url)
    transport = new LongPollingTransport(host)
    host.onOpen.mockImplementation(() => transport!.close())
    transport.connect(false)
    await vi.waitFor(() => expect(host.onOpen).toHaveBeenCalled())
    await tick(50)
    expect(host.onActivity).not.toHaveBeenCalled()
    expect(transport.ready).toBe(false)
  })

  it('stops when it is closed while handling a payload', async () => {
    server = await createMockServer()
    const host = createHost(server.url)
    transport = new LongPollingTransport(host)
    host.onData.mockImplementation(() => {
      transport!.close()
      return { shouldReconnect: true }
    })
    transport.connect(false)
    await vi.waitFor(() => expect(host.onData).toHaveBeenCalled())
    await tick(50)
    expect(host.onReconnecting).not.toHaveBeenCalled()
    expect(requests('poll')).toHaveLength(0)
  })

  it('does not report "reconnected" when closed right after a reconnect request', async () => {
    server = await createMockServer()
    const host = createHost(server.url)
    transport = new LongPollingTransport(host)
    host.onData.mockReturnValue({ shouldReconnect: true })
    host.onReconnecting.mockImplementation(() => transport!.close())
    transport.connect(false)
    await vi.waitFor(() => expect(host.onReconnecting).toHaveBeenCalled())
    await tick(50)
    expect(host.onReconnected).not.toHaveBeenCalled()
  })

  it('raises "reconnected" at once when a poll succeeds after errors', async () => {
    server = await createMockServer()
    server.overrides.push({ path: '/signalr/reconnect', status: 500 })
    const host = createHost(server.url)
    transport = new LongPollingTransport(host)
    transport.connect(true)
    // Resumed connection: the first reconnect poll fails and the retry is
    // answered by a payload.
    await vi.waitFor(() => expect(host.onReconnecting).toHaveBeenCalled())
    await vi.waitFor(() => expect(requests('reconnect')).toHaveLength(2))
    server.push('{"C":"d-2"}')
    await vi.waitFor(() =>
      expect(host.onData).toHaveBeenCalledWith('{"C":"d-2"}')
    )
    expect(host.onOpen).toHaveBeenCalledTimes(1)
    expect(host.onError).toHaveBeenCalledTimes(1)
  })

  it('opens a resumed connection after the backoff delay when the server holds the poll', async () => {
    server = await createMockServer()
    server.overrides.push({ path: '/signalr/reconnect', status: 500 })
    // A large attempt count delays the initial "open" timer.
    const host = createHost(server.url, { reconnectAttempt: 5 })
    transport = new LongPollingTransport(host)
    transport.connect(true)
    await vi.waitFor(() => expect(host.onReconnecting).toHaveBeenCalled())
    // The server keeps the second poll: the backoff timer opens it (1 s).
    await vi.waitFor(() => expect(host.onOpen).toHaveBeenCalledTimes(1), {
      timeout: 3000
    })
    expect(host.onReconnected).not.toHaveBeenCalled()
  })

  it('fails instead of retrying when the connection is not up', async () => {
    server = await createMockServer()
    server.overrides.push({ path: '/signalr/connect', status: 500 })
    const host = createHost(server.url, { connecting: true })
    transport = new LongPollingTransport(host)
    transport.connect(false)
    await vi.waitFor(() => expect(host.onClose).toHaveBeenCalledTimes(1))
    expect(host.onReconnecting).not.toHaveBeenCalled()
  })

  it('sends form encoded bodies', async () => {
    server = await createMockServer()
    const host = createHost(server.url, {
      request: vi.fn((endpoint: string) => ({
        url: `${server!.url}/${endpoint}?transport=longPolling`,
        body: 'messageId=d-1'
      }))
    })
    transport = new LongPollingTransport(host)
    transport.connect(false)
    await vi.waitFor(() => expect(requests('connect')).toHaveLength(1))
    expect(requests('connect')[0]?.body.get('messageId')).toBe('d-1')
  })
})

describe('server-sent events transport', () => {
  it('fails when the server cannot be reached', async () => {
    const host = createHost('http://127.0.0.1:1')
    transport = new ServerSentEventsTransport(host)
    transport.connect(false)
    await vi.waitFor(() => expect(host.onClose).toHaveBeenCalledTimes(1))
    expect(host.onClose.mock.calls[0]?.[0]).toBeInstanceOf(Error)
  })

  it('stops reading when it is closed while receiving', async () => {
    server = await createMockServer()
    const host = createHost(server.url)
    transport = new ServerSentEventsTransport(host)
    host.onActivity.mockImplementation(() => transport!.close())
    transport.connect(false)
    await vi.waitFor(() => expect(host.onActivity).toHaveBeenCalled())
    await tick(50)
    expect(host.onData).not.toHaveBeenCalled()
    expect(host.onClose).not.toHaveBeenCalled()
  })

  it('stops reading when it is closed while handling a payload', async () => {
    server = await createMockServer()
    const host = createHost(server.url)
    transport = new ServerSentEventsTransport(host)
    host.onData.mockImplementation(() => transport!.close())
    transport.connect(false)
    await vi.waitFor(() => expect(host.onData).toHaveBeenCalled())
    server.push('{"C":"d-2"}')
    await tick(50)
    expect(host.onData).toHaveBeenCalledTimes(1)
  })

  it('reports the end of the stream', async () => {
    server = await createMockServer()
    const host = createHost(server.url)
    transport = new ServerSentEventsTransport(host)
    transport.connect(false)
    await vi.waitFor(() => expect(host.onOpen).toHaveBeenCalled())
    server.endStreams()
    await vi.waitFor(() => expect(host.onClose).toHaveBeenCalledTimes(1))
    expect(host.onClose.mock.calls[0]?.[0]).toBeUndefined()
  })
})

describe('websocket transport', () => {
  it('ignores binary frames', async () => {
    server = await createMockServer()
    const host = createHost(server.url)
    transport = new WebSocketTransport(host)
    transport.connect(false)
    await vi.waitFor(() => expect(host.onOpen).toHaveBeenCalled())
    server.pushBinary('binary')
    server.push('text')
    await vi.waitFor(() => expect(host.onData).toHaveBeenCalledWith('text'))
    expect(host.onData).not.toHaveBeenCalledWith('binary')
  })

  it('gives up on handshakes that take too long', async () => {
    // Accepts the TCP connection but never answers the upgrade.
    const sockets = new Set<net.Socket>()
    const silent = net.createServer((socket) => {
      sockets.add(socket)
      socket.on('error', () => {})
    })
    await new Promise<void>((resolve) => silent.listen(0, '127.0.0.1', resolve))
    const { port } = silent.address() as net.AddressInfo
    try {
      const host = createHost(`http://127.0.0.1:${port}/signalr`, {
        requestTimeout: 50
      })
      transport = new WebSocketTransport(host)
      transport.connect(false)
      await vi.waitFor(() => expect(host.onClose).toHaveBeenCalledTimes(1))
      expect(host.onOpen).not.toHaveBeenCalled()
    } finally {
      for (const socket of sockets) socket.destroy()
      await new Promise<void>((resolve) => silent.close(() => resolve()))
    }
  })
})
