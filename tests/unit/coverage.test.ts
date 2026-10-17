import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  Client,
  HubError,
  SignalRError,
  type TransportName
} from '../../src/index.js'
import {
  createMockServer,
  NEGOTIATE_BODY,
  type MockServer
} from '../helpers/mock-server.js'

let server: MockServer | undefined
let client: Client | undefined

afterEach(async () => {
  vi.restoreAllMocks()
  client?.end()
  client = undefined
  await server?.close()
  server = undefined
})

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const internals = (c: Client): any => c

function once<T extends unknown[]>(
  register: (cb: (...args: T) => void) => void
): Promise<T> {
  return new Promise((resolve) => register((...args) => resolve(args)))
}

function create(transports?: TransportName[]): Client {
  client = new Client(server!.url, ['Hub'])
  client.reconnectDelayTime = 10
  client.longPollReconnectDelay = 20
  if (transports) client.transports = transports
  return client
}

async function connect(
  transports?: TransportName[],
  setup?: (c: Client) => void
): Promise<Client> {
  const c = create(transports)
  setup?.(c)
  const connected = once((cb) => c.on('connected', cb))
  c.start()
  await connected
  return c
}

/** Collects the errors of the client. */
function errors(c: Client): SignalRError[] {
  const list: SignalRError[] = []
  c.on('error', (e) => list.push(e))
  return list
}

const requests = (path: string): MockServer['requests'] =>
  server!.requests.filter((r) => r.path === `/signalr/${path}`)

const tick = (ms: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, ms))

const codes = (list: SignalRError[]): string[] => list.map((e) => e.code)

describe('negotiate', () => {
  it('reports an error message sent by the server', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: { ...NEGOTIATE_BODY, Error: 'nope' }
    }
    const c = create()
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(codes(list)).toContain('ERR_PROTOCOL'))
    expect(list[0]?.message).toContain('nope')
  })

  it('gives up after too many redirects', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: { ProtocolVersion: '2.1', RedirectUrl: server.url }
    }
    const c = create()
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(codes(list)).toContain('ERR_NEGOTIATE'))
    expect(list[0]?.message).toContain('redirection limit')
    expect(requests('negotiate').length).toBeGreaterThanOrEqual(101)
  })

  it('works without keep-alives', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: { ...NEGOTIATE_BODY, KeepAliveTimeout: 0 }
    }
    await connect(['webSockets'])
    expect(internals(client!).keepAliveTimeout).toBe(0)
  })

  it('ignores a negotiate that finishes after the client stopped', async () => {
    server = await createMockServer()
    server.delays.negotiate = 100
    const c = create()
    c.start()
    c.end()
    await new Promise((resolve) => setTimeout(resolve, 250))
    expect(requests('connect')).toHaveLength(0)
  })

  it('ignores a start that finishes after the client stopped', async () => {
    server = await createMockServer()
    server.delays.start = 100
    const c = create(['serverSentEvents'])
    const connected = vi.fn()
    c.on('connected', connected)
    c.start()
    await vi.waitFor(() => expect(requests('start')).toHaveLength(1))
    c.end()
    await new Promise((resolve) => setTimeout(resolve, 250))
    expect(connected).not.toHaveBeenCalled()
  })

  it('reports a start request with a bad status and retries', async () => {
    server = await createMockServer()
    server.start = { status: 500, body: '' }
    const c = create(['serverSentEvents'])
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(codes(list)).toContain('ERR_START'))
    await vi.waitFor(() => expect(requests('negotiate').length).toBeGreaterThan(1))
  })
})

describe('start response', () => {
  it('reports an unreadable start response', async () => {
    server = await createMockServer()
    server.start = { status: 200, body: 'garbage' }
    const c = create(['serverSentEvents'])
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(list[0]?.message).toContain('parsing start'))
  })

  it('creates hub errors with defaults', () => {
    const error = new HubError('x')
    expect(error.isHubError).toBe(false)
    expect(error.data).toBeUndefined()
  })
})

describe('start', () => {
  it('ignores a second start and invalid urls', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'])
    c.start()
    expect(requests('negotiate')).toHaveLength(1)

    const bad = new Client('not a url', ['Hub'])
    const list = errors(bad)
    bad.start()
    expect(codes(list)).toEqual(['INVALID_URL'])
  })
})

describe('ping', () => {
  it('does nothing while the connection is not up', async () => {
    server = await createMockServer()
    const c = create()
    await internals(c)._ping()
    expect(requests('ping')).toHaveLength(0)
  })

  it('reports failing and invalid pings', async () => {
    server = await createMockServer()
    server.overrides.push(
      { path: '/signalr/ping', status: 500 },
      { path: '/signalr/ping', status: 200, body: 'garbage' },
      { path: '/signalr/ping', status: 200, body: '{"Response":"nope"}' }
    )
    const c = create(['serverSentEvents'])
    c.pingInterval = 20
    const list = errors(c)
    const connected = once((cb) => c.on('connected', cb))
    c.start()
    await connected
    await vi.waitFor(() => expect(list.length).toBeGreaterThanOrEqual(3))
    expect(codes(list).slice(0, 3)).toEqual(['ERR_PING', 'ERR_PING', 'ERR_PING'])
  })

  it('wraps network errors of a ping', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'])
    const list = errors(c)
    internals(c).requestTimeout = 1
    server.delays.negotiate = 0
    // Point the ping at a closed port.
    internals(c).baseUrl = 'http://127.0.0.1:1'
    await internals(c)._ping()
    expect(codes(list)).toEqual(['ERR_PING'])
  })
})

describe('messages', () => {
  it('ignores empty frames, unknown results, unknown methods and a second init', async () => {
    server = await createMockServer()
    const c = await connect(['webSockets'])
    const list = errors(c)
    server.push('')
    server.push('null')
    server.push(JSON.stringify({ I: '99', R: 1 }))
    server.push(JSON.stringify({ C: 'd-2', M: [{ H: 'Hub', M: 'Nope', A: [] }] }))
    server.push(JSON.stringify({ C: 'd-3', S: 1, M: [] }))
    await new Promise((resolve) => setTimeout(resolve, 100))
    expect(list).toHaveLength(0)
    expect(requests('start')).toHaveLength(1)
    expect(c.connection.state).toBe('Connected')
  })

  it('stops on an unreadable frame once connected', async () => {
    server = await createMockServer()
    const c = await connect(['webSockets'])
    const list = errors(c)
    server.push('not json')
    await vi.waitFor(() => expect(codes(list)).toEqual(['ERR_PARSE']))
    await vi.waitFor(() => expect(c.connection.state).toBe('Disconnected'))
  })

  it('falls back when the init message is unreadable', async () => {
    server = await createMockServer()
    server.initRaw = 'not json'
    const c = create(['webSockets', 'serverSentEvents'])
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(codes(list)).toContain('ERR_CONNECT'))
    expect(requests('connect').length).toBeGreaterThan(1)
  })

  it('rethrows errors of hub handlers on the next tick', async () => {
    server = await createMockServer()
    const c = await connect(['webSockets'])
    const boom = new Error('boom')
    c.connection.hub.on('Hub', 'Tick', () => {
      throw boom
    })
    const thrown: unknown[] = []
    const original = process.nextTick.bind(process)
    vi.spyOn(process, 'nextTick').mockImplementation(((
      cb: () => void,
      ...args: unknown[]
    ) => {
      if (String(cb).includes('throw')) {
        try {
          cb()
        } catch (e) {
          thrown.push(e)
        }
        return
      }
      original(cb, ...(args as []))
    }) as typeof process.nextTick)

    server.push(JSON.stringify({ C: 'd-2', M: [{ H: 'Hub', M: 'Tick', A: [] }] }))
    await vi.waitFor(() => expect(thrown).toEqual([boom]))
    expect(c.connection.state).toBe('Connected')
  })

  it('honours the poll delay of long polling', async () => {
    server = await createMockServer()
    const c = await connect(['longPolling'])
    const got = vi.fn()
    c.connection.hub.on('Hub', 'Tick', got)
    server.push(JSON.stringify({ C: 'd-2', L: 30, M: [] }))
    await vi.waitFor(() => expect(requests('poll').length).toBeGreaterThan(1))
    server.push(JSON.stringify({ C: 'd-3', M: [{ H: 'Hub', M: 'Tick', A: [] }] }))
    await vi.waitFor(() => expect(got).toHaveBeenCalled())
  })
})

describe('sending', () => {
  it('turns function and undefined arguments into null', async () => {
    server = await createMockServer()
    const sent: string[] = []
    server.onClientMessage = (text) => {
      sent.push(text)
      return JSON.stringify({ I: '0', R: null })
    }
    const c = await connect(['serverSentEvents'])
    await c.connection.hub.call('Hub', 'x', () => {}, undefined, 1)
    expect(JSON.parse(sent[0]!).A).toEqual([null, null, 1])
  })

  it('reports sends that fail without a pending call', async () => {
    server = await createMockServer()
    const c = create(['serverSentEvents'])
    const list = errors(c)
    // Not started yet.
    c.connection.hub.invoke('Hub', 'x')
    expect(codes(list)).toEqual(['ERR_SEND'])

    const connected = once((cb) => c.on('connected', cb))
    c.start()
    await connected
    server.overrides.push({ path: '/signalr/send', status: 500 })
    c.connection.hub.invoke('Hub', 'x')
    await vi.waitFor(() => expect(codes(list)).toEqual(['ERR_SEND', 'ERR_SEND']))
  })

  it('rejects a pending call when its send fails', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'])
    server.overrides.push({ path: '/signalr/send', status: 500 })
    await expect(c.connection.hub.call('Hub', 'x')).rejects.toBeInstanceOf(
      SignalRError
    )
  })

  it('times out calls that never get a result', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'], (cl) => {
      cl.callTimeout = 30
    })
    await expect(c.connection.hub.call('Hub', 'x')).rejects.toBe('Timeout')
  })

  it('does not send over a WebSocket that is not open', async () => {
    server = await createMockServer()
    const c = await connect(['webSockets'])
    const transport = internals(c).transport
    await expect(transport.send('{}')).resolves.toBeUndefined()
    transport.close()
    await expect(transport.send('{}')).rejects.toThrow('not open')
  })

  it('surfaces hub errors without hub data', async () => {
    server = await createMockServer()
    server.onClientMessage = () => JSON.stringify({ I: '0', E: 'bad' })
    const c = await connect(['serverSentEvents'])
    await expect(c.connection.hub.call('Hub', 'x')).rejects.toMatchObject({
      isHubError: false
    })
    expect(HubError).toBeDefined()
  })
})

describe('connection handling', () => {
  it('renegotiates when the init message carried no message id', async () => {
    server = await createMockServer()
    server.init = { S: 1, M: [] }
    const c = await connect(['webSockets'])
    const connected = once((cb) => c.on('connected', cb))
    server.dropSockets()
    await connected
    expect(requests('negotiate')).toHaveLength(2)
  })

  it('restarts when the transport is lost during the start request', async () => {
    server = await createMockServer()
    server.delays.start = 150
    const c = create(['serverSentEvents'])
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(requests('start')).toHaveLength(1))
    const transport = internals(c).transport
    transport.close()
    internals(c)._onTransportClosed(transport, new Error('lost'))
    await vi.waitFor(() => expect(codes(list)).toContain('ERR_START'))
    await vi.waitFor(() => expect(requests('negotiate').length).toBeGreaterThan(1))
  })

  it('ignores callbacks of transports that are no longer current', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'])
    const before = c.connection.state
    internals(c)._onTransportReconnecting(undefined)
    internals(c)._onTransportReconnecting({})
    internals(c)._onTransportReconnected(undefined)
    internals(c)._onTransportReconnected({})
    internals(c)._onTransportClosed(undefined)
    internals(c)._onTransportClosed({})
    internals(c)._onTransportOpen(undefined)
    expect(c.connection.state).toBe(before)
  })

  it('schedules a single reconnect at a time', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'])
    const reconnecting = vi.fn()
    c.on('reconnecting', reconnecting)
    internals(c)._reconnect()
    internals(c)._reconnect()
    await vi.waitFor(() => expect(reconnecting).toHaveBeenCalledTimes(1))
  })

  it('renegotiates after being lost for longer than the disconnect timeout', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'])
    internals(c).disconnectTimeout = 1
    const connected = once((cb) => c.on('connected', cb))
    server.dropSockets()
    internals(c).connectionLostAt = Date.now() - 1000
    await connected
    expect(requests('negotiate').length).toBeGreaterThan(1)
  })

  it('reports an unreadable negotiate redirect target', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: { ProtocolVersion: '2.1', RedirectUrl: 'http://' }
    }
    const c = create()
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(list.length).toBeGreaterThan(0))
  })
})

describe('long polling retries', () => {
  it('backs off "reconnected" while polls keep failing', async () => {
    server = await createMockServer()
    const c = await connect(['longPolling'])
    const list = errors(c)
    // Two failing polls in a row, then it recovers.
    server.overrides.push(
      { path: '/signalr/reconnect', status: 500 },
      { path: '/signalr/reconnect', status: 500 }
    )
    const connected = once((cb) => c.on('connected', cb))
    server.dropSockets()
    await connected
    expect(codes(list)[0]).toBe('ERR_SOCKET')
    expect(requests('reconnect').length).toBeGreaterThanOrEqual(3)
    expect(requests('negotiate')).toHaveLength(1)
  })

  it('fails over to the next transport when the first poll fails', async () => {
    server = await createMockServer()
    server.overrides.push({ path: '/signalr/connect', status: 500 })
    const c = create(['longPolling'])
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(codes(list)).toContain('ERR_CONNECT'))
  })

  it('falls back to the client when the connection was silent for too long', async () => {
    server = await createMockServer()
    const c = await connect(['longPolling'])
    internals(c).disconnectTimeout = 0
    c.connection.lastMessageAt = 0
    const disconnected = once((cb) => c.on('disconnected', cb))
    server.dropSockets()
    expect((await disconnected)[0]).toBe('failed')
  })

  it('stops retrying when it is closed during the wait', async () => {
    server = await createMockServer()
    const c = create(['longPolling'])
    c.longPollReconnectDelay = 10_000
    const connected = once((cb) => c.on('connected', cb))
    c.start()
    await connected
    const reconnecting = once((cb) => c.on('reconnecting', cb))
    server.dropSockets()
    await reconnecting
    c.end()
    await new Promise((resolve) => setTimeout(resolve, 50))
    expect(requests('reconnect')).toHaveLength(0)
  })

  it('opens a resumed connection after the backoff when the server holds the request', async () => {
    server = await createMockServer()
    const c = await connect(['longPolling'])
    internals(c).reconnectCount = 1
    const connected = once((cb) => c.on('connected', cb))
    internals(c)._lose()
    await connected
    expect(requests('reconnect').length).toBeGreaterThanOrEqual(1)
  })

  it('times out polls that the server never answers', async () => {
    server = await createMockServer()
    // ConnectionTimeout + 10 seconds = 50 ms.
    server.negotiate = {
      status: 200,
      body: { ...NEGOTIATE_BODY, ConnectionTimeout: -9.95 }
    }
    const c = create(['longPolling'])
    const connected = once((cb) => c.on('connected', cb))
    c.start()
    await connected
    const reconnecting = once((cb) => c.on('reconnecting', cb))
    await reconnecting
  })
})

describe('server-sent events', () => {
  it('fails when the stream cannot be opened', async () => {
    server = await createMockServer()
    server.failTransports.add('serverSentEvents')
    const c = create(['serverSentEvents'])
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(codes(list)).toContain('ERR_CONNECT'))
  })

  it('handles split events and comments', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'])
    const got = vi.fn()
    c.connection.hub.on('Hub', 'Tick', got)
    server.push(JSON.stringify({ C: 'd-2', M: [{ H: 'Hub', M: 'Tick', A: [] }] }))
    await vi.waitFor(() => expect(got).toHaveBeenCalledTimes(1))
  })
})

describe('websockets', () => {
  it('fails when the upgrade is refused', async () => {
    server = await createMockServer()
    server.failTransports.add('webSockets')
    const c = create(['webSockets'])
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(codes(list)).toContain('ERR_CONNECT'))
  })
})

describe('negotiate defaults and variants', () => {
  it('accepts a minimal negotiate response of an older protocol', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: {
        ConnectionToken: 't',
        ConnectionId: 'i',
        ProtocolVersion: '1.4',
        TryWebSockets: true
      }
    }
    const c = create(['serverSentEvents'])
    c.qs = { custom: '1' }
    const connected = once((cb) => c.on('connected', cb))
    c.start()
    await connected
    expect(internals(c).pollTimeout).toBe(120_000)
    expect(internals(c).disconnectTimeout).toBe(30_000)
    expect(internals(c).totalTransportConnectTimeout).toBe(0)
    await internals(c)._ping()
    expect(requests('ping')[0]?.query.get('custom')).toBe('1')
  })

  it('reports that no transport was selected', async () => {
    server = await createMockServer()
    const c = create([])
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(codes(list)).toContain('NO_TRANSPORT'))
    expect(list[0]?.message).toBe('No transports were selected.')
  })

  it('uses default timeouts when none are configured', async () => {
    server = await createMockServer()
    server.onClientMessage = () => JSON.stringify({ I: '0', R: 1 })
    const c = create(['serverSentEvents'])
    c.requestTimeout = 0
    c.callTimeout = 0
    const connected = once((cb) => c.on('connected', cb))
    c.start()
    await connected
    await expect(c.connection.hub.call('Hub', 'x')).resolves.toBe(1)
  })

  it('ignores a negotiate failure that arrives after the client stopped', async () => {
    server = await createMockServer()
    server.negotiate = { status: 500, body: '' }
    server.delays.negotiate = 100
    const c = create()
    const list = errors(c)
    c.start()
    c.end()
    await tick(250)
    expect(list).toHaveLength(0)
  })

  it('ignores a negotiate that succeeds for a run that was replaced', async () => {
    server = await createMockServer()
    server.delays.negotiate = 100
    const c = create()
    c.start()
    internals(c).runId++
    await tick(250)
    expect(requests('connect')).toHaveLength(0)
  })

  it('ignores a start that succeeds for a run that was replaced', async () => {
    server = await createMockServer()
    server.delays.start = 150
    const c = create(['serverSentEvents'])
    const connected = vi.fn()
    c.on('connected', connected)
    c.start()
    await vi.waitFor(() => expect(requests('start')).toHaveLength(1))
    internals(c).runId++
    await tick(300)
    expect(connected).not.toHaveBeenCalled()
  })
})

describe('more connection handling', () => {
  it('uses the default reconnect delay', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'], (cl) => {
      cl.reconnectDelayTime = 0
    })
    const started = Date.now()
    const connected = once((cb) => c.on('connected', cb))
    server.dropSockets()
    await connected
    expect(Date.now() - started).toBeGreaterThanOrEqual(1900)
  }, 10_000)

  it('ignores "reconnected" when the connection is not reconnecting', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'])
    const connected = vi.fn()
    c.on('connected', connected)
    internals(c)._onTransportReconnected(internals(c).transport)
    expect(connected).not.toHaveBeenCalled()
  })

  it('exhausts the transports without an error to report', async () => {
    server = await createMockServer()
    server.delays.start = 1000
    const c = create(['serverSentEvents'])
    const list = errors(c)
    // The transport is replaced before the init message arrives.
    server.init = { C: 'd-1', M: [] }
    c.start()
    await vi.waitFor(() => expect(requests('connect')).toHaveLength(1))
    internals(c)._onTransportClosed(internals(c).transport)
    await vi.waitFor(() => expect(codes(list)).toContain('ERR_CONNECT'))
    expect(list.find((e) => e.code === 'ERR_CONNECT')?.message).toBe(
      'Fallback transports exhausted.'
    )
  })

  it('resumes after the stream ended cleanly', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'])
    const connected = once((cb) => c.on('connected', cb))
    server.endStreams()
    await connected
    expect(requests('reconnect')).toHaveLength(1)
  })

  it('rejects sends while the connection is still starting', async () => {
    server = await createMockServer()
    server.delays.start = 300
    const c = create(['serverSentEvents'])
    const list = errors(c)
    c.start()
    await vi.waitFor(() => expect(internals(c).transport?.ready).toBe(true))
    expect(c.connection.state).toBe('Connecting')
    c.connection.hub.invoke('Hub', 'x')
    expect(codes(list)).toEqual(['ERR_SEND'])
    expect(requests('send')).toHaveLength(0)
  })

  it('registers several methods on one hub', async () => {
    server = await createMockServer()
    const c = await connect(['serverSentEvents'])
    const one = vi.fn()
    const two = vi.fn()
    c.connection.hub.on('Hub', 'One', one)
    c.connection.hub.on('Hub', 'Two', two)
    server.push(
      JSON.stringify({
        C: 'd-2',
        M: [
          { H: 'Hub', M: 'One', A: [] },
          { H: 'Hub', M: 'Two', A: [] }
        ]
      })
    )
    await vi.waitFor(() => expect(two).toHaveBeenCalled())
    expect(one).toHaveBeenCalled()
  })
})
