import net from 'node:net'
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
  client?.end()
  client = undefined
  await server?.close()
  server = undefined
})

function once<T extends unknown[]>(
  register: (cb: (...args: T) => void) => void
): Promise<T> {
  return new Promise((resolve) => register((...args) => resolve(args)))
}

/** Creates the client, starts it and resolves once it is connected. */
async function connect(
  transports?: TransportName[],
  setup?: (c: Client) => void
): Promise<Client> {
  client = new Client(server!.url, ['Hub'])
  client.reconnectDelayTime = 10
  if (transports) client.transports = transports
  setup?.(client)
  const connected = once((cb) => client!.on('connected', cb))
  client.start()
  await connected
  return client
}

const requests = (path: string): MockServer['requests'] =>
  server!.requests.filter((r) => r.path === `/signalr/${path}`)

describe('protocol', () => {
  it('sends client protocol 2.1 and the hubs on every request', async () => {
    server = await createMockServer()
    await connect(['webSockets'])

    for (const path of ['negotiate', 'connect', 'start']) {
      const [request] = requests(path)
      expect(request?.query.get('clientProtocol')).toBe('2.1')
      expect(request?.query.get('connectionData')).toBe('[{"name":"hub"}]')
    }
    expect(requests('negotiate')[0]?.query.has('connectionToken')).toBe(false)
    expect(requests('connect')[0]?.query.get('connectionToken')).toBe('token')
    expect(requests('start')[0]?.query.get('connectionToken')).toBe('token')
    expect(requests('start')[0]?.query.get('transport')).toBe('webSockets')
    // tid is a cache buster of the persistent connection requests.
    expect(Number(requests('connect')[0]?.query.get('tid'))).toBeLessThanOrEqual(
      10
    )
  })

  it('forwards the cookies of negotiate to the other requests', async () => {
    server = await createMockServer()
    await connect(['webSockets'])

    expect(requests('negotiate')[0]?.cookie).toBeUndefined()
    expect(requests('connect')[0]?.cookie).toBe('affinity=abc')
    expect(requests('start')[0]?.cookie).toBe('affinity=abc')
  })

  it('waits for the init message before sending start', async () => {
    server = await createMockServer()
    await connect(['webSockets'])

    expect(server.requests.map((r) => r.path)).toEqual([
      '/signalr/negotiate',
      '/signalr/connect',
      '/signalr/start'
    ])
  })

  it('processes the messages of the init message and buffers them until connected', async () => {
    server = await createMockServer()
    server.init = {
      C: 'd-1',
      S: 1,
      M: [{ H: 'Hub', M: 'Hello', A: ['early'] }]
    }
    const events: string[] = []
    await connect(['webSockets'], (c) => {
      c.connection.hub.on('hub', 'hello', (message) => {
        events.push(`message:${message}`)
      })
      c.on('connected', () => events.push('connected'))
    })

    // The message arrived while connecting, it is delivered right before
    // the connected event.
    expect(events).toEqual(['message:early', 'connected'])
  })

  it('reports an invalid start response', async () => {
    server = await createMockServer()
    server.start = { status: 200, body: '{"Response":"nope"}' }
    client = new Client(server.url, ['Hub'])
    client.reconnectDelayTime = 10_000
    client.transports = ['webSockets']

    const failed = once<[SignalRError]>((cb) => client!.on('error', cb))
    client.start()
    expect((await failed)[0].code).toBe('ERR_START')
  })

  it('follows negotiate redirects with their query string and token', async () => {
    server = await createMockServer()
    server.negotiateQueue = [
      {
        ProtocolVersion: '2.1',
        RedirectUrl: `${server.url}?foo=bar`,
        AccessToken: 'secret'
      }
    ]
    await connect(['serverSentEvents'])

    const negotiates = requests('negotiate')
    expect(negotiates).toHaveLength(2)
    expect(negotiates[1]?.query.get('foo')).toBe('bar')
    expect(negotiates[1]?.authorization).toBe('Bearer secret')
    expect(requests('start')[0]?.authorization).toBe('Bearer secret')
    // Server-sent events can't send headers in a browser: the token is also
    // in the query string.
    expect(requests('connect')[0]?.query.get('access_token')).toBe('secret')
  })

  it('rejects incompatible servers', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: { ...NEGOTIATE_BODY, ProtocolVersion: '1.0' }
    }
    client = new Client(server.url, ['Hub'])
    const failed = once<[SignalRError]>((cb) => client!.on('error', cb))
    const disconnected = once((cb) => client!.on('disconnected', cb))
    client.start()
    expect((await failed)[0].code).toBe('ERR_PROTOCOL')
    expect(await disconnected).toEqual(['failed'])
  })

  it('rejects ASP.NET Core servers', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: { availableTransports: [], connectionId: 'x' }
    }
    client = new Client(server.url, ['Hub'])
    const failed = once<[SignalRError]>((cb) => client!.on('error', cb))
    client.start()
    const [error] = await failed
    expect(error.code).toBe('ERR_PROTOCOL')
    expect(error.message).toContain('ASP.NET Core')
  })

  it('stops on a global server error', async () => {
    server = await createMockServer()
    await connect(['webSockets'])
    const failed = once<[SignalRError]>((cb) => client!.on('error', cb))
    const disconnected = once((cb) => client!.on('disconnected', cb))
    server.push(JSON.stringify({ E: 'boom' }))
    expect((await failed)[0].code).toBe('ERR_SERVER')
    expect(await disconnected).toEqual(['failed'])
    expect(client!.connection.state).toBe('Disconnected')
  })

  it('keeps the server session alive with /ping', async () => {
    server = await createMockServer()
    await connect(['webSockets'], (c) => {
      c.pingInterval = 20
    })
    await vi.waitFor(() => expect(requests('ping').length).toBeGreaterThan(1))
  })

  it('stops when the ping is unauthorized', async () => {
    server = await createMockServer()
    server.pingStatus = 401
    const disconnected = once((cb) => {
      client = new Client(server!.url, ['Hub'])
      client.transports = ['webSockets']
      client.pingInterval = 20
      client.on('disconnected', cb)
      client.start()
    })
    expect(await disconnected).toEqual(['unauthorized'])
  })

  it('maps negotiate statuses and failures', async () => {
    for (const [status, code] of [
      [401, 'UNAUTHORIZED'],
      [302, 'UNAUTHORIZED'],
      [500, 'ERR_NEGOTIATE']
    ] as const) {
      server = await createMockServer()
      server.negotiate = { status, body: '' }
      client = new Client(server.url, ['Hub'])
      client.reconnectDelayTime = 60_000

      const failed = once<[SignalRError]>((cb) => client!.on('error', cb))
      client.start()
      expect((await failed)[0].code).toBe(code)
      client.end()
      await server.close()
    }
    server = undefined
  })

  it('reports an unparsable negotiate response', async () => {
    server = await createMockServer()
    server.negotiate = { status: 200, body: 'not json' }
    client = new Client(server.url, ['Hub'])
    client.reconnectDelayTime = 60_000

    const failed = once<[SignalRError]>((cb) => client!.on('error', cb))
    client.start()
    expect((await failed)[0].code).toBe('ERR_NEGOTIATE')
  })

  it('rejects servers without WebSocket support when only WebSockets are allowed', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: { ...NEGOTIATE_BODY, TryWebSockets: false }
    }
    client = new Client(server.url, ['Hub'])
    client.transports = ['webSockets']

    const failed = once<[SignalRError]>((cb) => client!.on('error', cb))
    client.start()
    expect((await failed)[0].code).toBe('UNSUPPORTED_WEBSOCKET')
  })

  it('times out slow negotiate requests', async () => {
    // Accepts connections but never answers.
    const held: net.Socket[] = []
    const hang = net.createServer((socket) => void held.push(socket))
    await new Promise<void>((r) => hang.listen(0, '127.0.0.1', r))
    const { port } = hang.address() as net.AddressInfo
    client = new Client(`http://127.0.0.1:${port}/signalr`, ['Hub'])
    client.requestTimeout = 100
    client.reconnectDelayTime = 60_000

    try {
      const failed = once<[SignalRError]>((cb) => client!.on('error', cb))
      client.start()
      const [error] = await failed
      expect(error.code).toBe('ERR_NEGOTIATE')
      expect(error.message).toBe('Timeout of 100ms exceeded.')
    } finally {
      client.end()
      for (const socket of held) socket.destroy()
      hang.close()
    }
  })

  it('validates url and hubs before connecting', () => {
    const errors: string[] = []
    for (const [url, hubs] of [
      ['', ['Hub']],
      ['ftp://example.com', ['Hub']],
      ['http://example.com', []]
    ] as const) {
      const c = new Client(url, [...hubs])
      c.on('error', (e) => errors.push(e.code))
      c.start()
    }
    expect(errors).toEqual(['INVALID_URL', 'INVALID_PROTOCOL', 'NO_HUB'])
  })
})

describe('hubs', () => {
  it.each(['webSockets', 'serverSentEvents', 'longPolling'] as const)(
    'resolves calls (invocation id 0 included) and surfaces hub errors over %s',
    async (transport) => {
      server = await createMockServer()
      server.onClientMessage = (text) => {
        const { I, M } = JSON.parse(text) as { I: number; M: string }
        return JSON.stringify(
          M === 'fail'
            ? { I: `${I}`, E: 'boom', H: true, D: { code: 1 } }
            : M === 'progress'
              ? { I: `P|${I}`, P: { I: `${I}`, D: 1 } }
              : { I: `${I}`, R: 'pong' }
        )
      }
      await connect([transport])
      const hub = client!.connection.hub

      await expect(hub.call('Hub', 'ping')).resolves.toBe('pong')
      const error = await hub.call('Hub', 'fail').catch((e: unknown) => e)
      expect(error).toBeInstanceOf(HubError)
      expect(error).toMatchObject({
        message: 'boom',
        isHubError: true,
        data: { code: 1 }
      })
    }
  )

  it('keeps an invocation pending on progress messages', async () => {
    server = await createMockServer()
    server.onClientMessage = (text) => {
      const { I } = JSON.parse(text) as { I: number }
      return JSON.stringify({ I: `P|${I}`, P: { I: `${I}`, D: 1 } })
    }
    await connect(['webSockets'], (c) => {
      c.callTimeout = 100
    })
    await expect(client!.connection.hub.call('Hub', 'work')).rejects.toBe(
      'Timeout'
    )
  })

  it('round-trips the hub state', async () => {
    server = await createMockServer()
    const sent: Record<string, unknown>[] = []
    server.onClientMessage = (text) => {
      const message = JSON.parse(text) as Record<string, unknown>
      sent.push(message)
      return JSON.stringify({ I: `${message['I']}`, R: null, S: { b: 2 } })
    }
    server.frames = [
      JSON.stringify({ C: 'd-2', M: [{ H: 'Hub', M: 'Tick', A: [], S: { a: 1 } }] })
    ]
    await connect(['webSockets'], (c) => {
      c.connection.hub.on('hub', 'tick', () => {})
    })
    await vi.waitFor(() =>
      expect(client!.connection.hub.state['hub']).toEqual({ a: 1 })
    )

    await client!.connection.hub.call('Hub', 'first')
    await client!.connection.hub.call('Hub', 'second')
    // The state from the server is sent back, then merged with the result's.
    expect(sent[0]?.['S']).toEqual({ a: 1 })
    expect(sent[1]?.['S']).toEqual({ a: 1, b: 2 })
  })

  it('rejects pending calls when the client stops', async () => {
    server = await createMockServer()
    await connect(['webSockets'], (c) => {
      c.callTimeout = 10_000
    })
    const pending = client!.connection.hub.call('Hub', 'never')
    client!.end()
    await expect(pending).rejects.toBeInstanceOf(SignalRError)
  })

  it('rejects calls made before the connection is started', async () => {
    server = await createMockServer()
    client = new Client(server.url, ['Hub'])
    await expect(client.connection.hub.call('Hub', 'x')).rejects.toMatchObject({
      code: 'ERR_SEND'
    })
  })
})

describe.each(['webSockets', 'serverSentEvents', 'longPolling'] as const)(
  '%s transport',
  (transport) => {
    it('connects and receives hub messages', async () => {
      server = await createMockServer()
      server.frames = [
        JSON.stringify({ C: 'd-2', M: [{ H: 'Hub', M: 'Hello', A: ['x'] }] })
      ]
      const message = new Promise<unknown>((resolve) => {
        client = new Client(server!.url, ['Hub'])
        client.transports = [transport]
        client.connection.hub.on('hub', 'hello', resolve)
        client.start()
      })

      expect(await message).toBe('x')
      const connect = requests('connect')[0]
      expect(connect?.query.get('transport')).toBe(transport)
      expect(requests('start')[0]?.query.get('transport')).toBe(transport)
    })

    it('resumes with /reconnect when the connection drops', async () => {
      server = await createMockServer()
      server.frames = [
        JSON.stringify({
          C: 'd-7',
          G: 'grp',
          M: [{ H: 'Hub', M: 'Tick', A: [] }]
        })
      ]
      const tick = new Promise<void>((resolve) => {
        client = new Client(server!.url, ['Hub'])
        client.connection.hub.on('hub', 'tick', () => resolve())
      })
      client!.transports = [transport]
      client!.reconnectDelayTime = 10
      const first = once((cb) => client!.on('connected', cb))
      client!.start()
      await first
      await tick

      const second = once((cb) => client!.on('connected', cb))
      server.dropSockets()
      await second

      await vi.waitFor(() => expect(requests('reconnect')).toHaveLength(1))
      const reconnect = requests('reconnect')[0]!
      expect(reconnect.query.get('transport')).toBe(transport)
      if (transport === 'longPolling') {
        // Long polling requests are POSTs carrying these in the body.
        expect(reconnect.method).toBe('POST')
        expect(reconnect.body.get('messageId')).toBe('d-7')
        expect(reconnect.body.get('groupsToken')).toBe('grp')
        expect(reconnect.query.has('tid')).toBe(false)
      } else {
        expect(reconnect.query.get('messageId')).toBe('d-7')
        expect(reconnect.query.get('groupsToken')).toBe('grp')
      }
      // The connection was resumed: no new negotiate nor start.
      expect(requests('negotiate')).toHaveLength(1)
      expect(requests('start')).toHaveLength(1)
    })
  }
)

describe('long polling', () => {
  it('polls with POST and the last message id', async () => {
    server = await createMockServer()
    server.frames = [JSON.stringify({ C: 'd-2', M: [] })]
    await connect(['longPolling'])
    await vi.waitFor(() => expect(requests('poll').length).toBeGreaterThan(1))

    const connectRequest = requests('connect')[0]!
    expect(connectRequest.method).toBe('POST')
    expect(connectRequest.query.has('tid')).toBe(false)
    const polls = requests('poll')
    expect(polls.every((p) => p.method === 'POST')).toBe(true)
    expect(polls[0]?.body.get('messageId')).toBe('d-1')
    expect(polls[1]?.body.get('messageId')).toBe('d-2')
  })

  it('is not monitored with keep-alives', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: { ...NEGOTIATE_BODY, KeepAliveTimeout: 0.2 }
    }
    const lost = vi.fn()
    await connect(['longPolling'], (c) => {
      c.on('disconnected', lost)
    })
    await new Promise((resolve) => setTimeout(resolve, 600))
    expect(lost).not.toHaveBeenCalled()
  })

  it('reconnects when the server asks to (T)', async () => {
    server = await createMockServer()
    await connect(['longPolling'])

    const reconnecting = once<[number]>((cb) => client!.on('reconnecting', cb))
    const connected = once((cb) => client!.on('connected', cb))
    server.push(JSON.stringify({ C: 'd-9', T: 1, M: [] }))
    expect((await reconnecting)[0]).toBe(1)
    await connected

    await vi.waitFor(() => expect(requests('reconnect')).toHaveLength(1))
    expect(requests('reconnect')[0]?.body.get('messageId')).toBe('d-9')
    // No new negotiation: the connection continues.
    expect(requests('negotiate')).toHaveLength(1)
  })
})

describe('long polling reconnect', () => {
  it('retries inside the transport and keeps the same connection', async () => {
    server = await createMockServer()
    await connect(['longPolling'])
    const reconnecting = vi.fn()
    const disconnected = vi.fn()
    const errors: unknown[] = []
    client!.on('reconnecting', reconnecting)
    client!.on('disconnected', disconnected)
    client!.on('error', (e) => errors.push(e))

    const connected = once((cb) => client!.on('connected', cb))
    const started = Date.now()
    server.dropSockets()
    await connected

    // The transport waits its own 3 seconds, not `reconnectDelayTime`.
    expect(Date.now() - started).toBeGreaterThanOrEqual(2900)
    expect(reconnecting).toHaveBeenCalledTimes(1)
    expect(disconnected).not.toHaveBeenCalled()
    expect(errors).toHaveLength(1)
    expect(requests('reconnect')).toHaveLength(1)
    expect(requests('negotiate')).toHaveLength(1)
  }, 15_000)
})

describe('reference client behaviours', () => {
  it('adds a cache-buster to GET requests only', async () => {
    server = await createMockServer()
    server.onClientMessage = (text) =>
      JSON.stringify({ I: `${(JSON.parse(text) as { I: number }).I}`, R: 1 })
    await connect(['serverSentEvents'])
    for (const path of ['negotiate', 'start']) {
      const [r] = requests(path)
      expect(r?.method).toBe('GET')
      expect(r?.query.get('_')).toMatch(/^\d+$/)
    }
    await client!.connection.hub.call('Hub', 'x')
    expect(requests('send')[0]?.query.has('_')).toBe(false)
  })

  it('sends the hub name in lower case', async () => {
    server = await createMockServer()
    const sent: string[] = []
    server.onClientMessage = (text) => {
      sent.push(text)
      const { I } = JSON.parse(text) as { I: number }
      return JSON.stringify({ I: `${I}`, R: 1 })
    }
    await connect(['serverSentEvents'])
    await client!.connection.hub.call('Hub', 'x')
    expect(JSON.parse(sent[0]!).H).toBe('hub')
  })

  it('stops the connection when a send response is unreadable', async () => {
    server = await createMockServer()
    server.onClientMessage = () => 'not json'
    await connect(['serverSentEvents'])
    const errors: SignalRError[] = []
    client!.on('error', (e) => errors.push(e))

    await expect(client!.connection.hub.call('Hub', 'x')).rejects.toBeInstanceOf(
      SignalRError
    )
    expect(errors.some((e) => e.code === 'ERR_PARSE')).toBe(true)
    await vi.waitFor(() => expect(requests('abort')).toHaveLength(1))
    expect(client!.connection.state).toBe('Disconnected')
  })
})

describe('keep-alive', () => {
  it('warns and reconnects when the server goes silent', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: { ...NEGOTIATE_BODY, KeepAliveTimeout: 0.3 }
    }
    await connect(['webSockets'])

    const slow = vi.fn()
    client!.on('connectionSlow', slow)
    const reconnecting = once<[number]>((cb) => client!.on('reconnecting', cb))
    await reconnecting
    expect(slow).toHaveBeenCalledTimes(1)
  })
})

describe('transport fallback', () => {
  it('falls back from WebSockets to server-sent events', async () => {
    server = await createMockServer()
    server.failTransports.add('webSockets')
    await connect()

    const transports = requests('connect')
      .filter((r) => r.method === 'GET')
      .map((r) => r.query.get('transport'))
    expect(server.requests.map((r) => r.method)).toContain('WS')
    expect(transports).toEqual(['serverSentEvents'])
  })

  it('falls back all the way to long polling', async () => {
    server = await createMockServer()
    server.failTransports.add('webSockets').add('serverSentEvents')
    await connect()

    const transports = requests('connect')
      .filter((r) => r.method !== 'WS')
      .map((r) => r.query.get('transport'))
    expect(transports).toEqual(['serverSentEvents', 'longPolling'])
  })

  it('falls back when the init message does not arrive in time', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: { ...NEGOTIATE_BODY, TransportConnectTimeout: 0.2 }
    }
    // Never sends the init message on WebSockets and server-sent events.
    server.init = { C: 'd-1' }
    const c = new Client(server.url, ['Hub'])
    client = c
    c.transports = ['webSockets', 'serverSentEvents']
    c.reconnectDelayTime = 60_000
    const failed = once<[SignalRError]>((cb) => c.on('error', cb))
    c.start()
    const [error] = await failed
    expect(error.code).toBe('ERR_CONNECT')
    expect(error.message).toContain('Fallback transports exhausted')
    expect(
      requests('connect').map((r) => r.query.get('transport'))
    ).toEqual(['webSockets', 'serverSentEvents'])
  })

  it('skips WebSockets when the server does not support them', async () => {
    server = await createMockServer()
    server.negotiate = {
      status: 200,
      body: { ...NEGOTIATE_BODY, TryWebSockets: false }
    }
    await connect()

    expect(server.requests.map((r) => r.method)).not.toContain('WS')
    expect(requests('connect')[0]?.query.get('transport')).toBe(
      'serverSentEvents'
    )
  })
})
