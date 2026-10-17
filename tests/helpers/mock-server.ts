import { createHash } from 'node:crypto'
import http from 'node:http'
import type { AddressInfo, Socket } from 'node:net'

export type MockRequest = {
  method: string
  path: string
  query: URLSearchParams
  /** Form body of POST requests. */
  body: URLSearchParams
  cookie: string | undefined
  authorization: string | undefined
}

export type MockServer = {
  url: string
  requests: MockRequest[]
  /** The init message: `{ C, S: 1, M: [] }` by default. */
  init: Record<string, unknown>
  /** Payloads delivered after the init message of a fresh /connect. */
  frames: string[]
  /** Reply to a client message: the response body of /send (SSE / long
   * polling) or a frame pushed on the socket (WebSocket). */
  onClientMessage: ((text: string) => string | undefined) | undefined
  /** Transports that the server refuses to open. */
  failTransports: Set<string>
  /** Negotiate responses consumed in order, before `negotiate`. */
  negotiateQueue: unknown[]
  negotiate: { status: number; body: unknown }
  /** Body of /start. */
  start: { status: number; body: string }
  /** Status of /ping. */
  pingStatus: number
  /** One-shot replies for the next request to a path, consumed in order. */
  overrides: { path: string; status: number; body?: string }[]
  /** Delays (ms) before answering negotiate and start. */
  delays: { negotiate: number; start: number }
  /** Raw init message, replaces `init` when set. */
  initRaw: string | undefined
  /** Push a payload to the connected client (any transport). */
  push(payload: string): void
  /** Cleanly ends every open server-sent events stream. */
  endStreams(): void
  /** Pushes a binary WebSocket frame. */
  pushBinary(payload: string): void
  /** Abruptly drops every open TCP connection. */
  dropSockets(): void
  close(): Promise<void>
}

function textFrame(payload: string): Buffer {
  const data = Buffer.from(payload)
  if (data.length > 125) throw new Error('mock frame too large')
  return Buffer.concat([Buffer.from([0x81, data.length]), data])
}

function unmask(chunk: Buffer): { opcode: number; text: string } {
  const opcode = (chunk[0] ?? 0) & 0x0f
  const len = (chunk[1] ?? 0) & 0x7f
  const mask = chunk.subarray(2, 6)
  const payload = Buffer.from(chunk.subarray(6, 6 + len))
  for (let i = 0; i < payload.length; i++) {
    payload[i] = (payload[i] ?? 0) ^ (mask[i % 4] ?? 0)
  }
  return { opcode, text: payload.toString('utf8') }
}

const GUID = '258EAFA5-E914-47DA-95CA-C5AB0DC85B11'

export const NEGOTIATE_BODY = {
  Url: '/signalr',
  ConnectionToken: 'token',
  ConnectionId: 'id',
  KeepAliveTimeout: 20,
  DisconnectTimeout: 30,
  ConnectionTimeout: 110,
  TryWebSockets: true,
  ProtocolVersion: '2.1',
  TransportConnectTimeout: 5,
  LongPollDelay: 0
}

/**
 * Minimal ASP.NET SignalR (classic) server speaking WebSockets, server-sent
 * events and long polling.
 */
export async function createMockServer(): Promise<MockServer> {
  const connections = new Set<Socket>()
  const pushes = new Set<(payload: string) => void>()
  const binaries = new Set<(payload: string) => void>()
  const streams = new Set<() => void>()
  const pollQueue: string[] = []
  let waitingPoll: ((payload: string) => void) | undefined

  const mock: MockServer = {
    url: '',
    requests: [],
    init: { C: 'd-1', S: 1, M: [] },
    frames: [],
    onClientMessage: undefined,
    failTransports: new Set(),
    negotiateQueue: [],
    negotiate: { status: 200, body: NEGOTIATE_BODY },
    start: { status: 200, body: '{"Response":"started"}' },
    pingStatus: 200,
    overrides: [],
    delays: { negotiate: 0, start: 0 },
    initRaw: undefined,
    push: (payload) => {
      if (waitingPoll) {
        const resolve = waitingPoll
        waitingPoll = undefined
        resolve(payload)
      } else pollQueue.push(payload)
      for (const send of pushes) send(payload)
    },
    endStreams: () => {
      for (const end of streams) end()
      streams.clear()
    },
    pushBinary: (payload) => {
      for (const send of binaries) send(payload)
    },
    dropSockets: () => {
      for (const socket of connections) socket.destroy()
      connections.clear()
    },
    close: async () => {}
  }

  const record = (method: string, req: http.IncomingMessage): MockRequest => {
    const [path = '', search = ''] = (req.url ?? '').split('?')
    const entry: MockRequest = {
      method,
      path,
      query: new URLSearchParams(search),
      body: new URLSearchParams(),
      cookie: req.headers.cookie,
      authorization: req.headers.authorization
    }
    mock.requests.push(entry)
    return entry
  }

  const server = http.createServer((req, res) => {
    const entry = record(req.method ?? '', req)
    const { path, query } = entry
    const transport = query.get('transport')

    let raw = ''
    req.on('data', (chunk) => (raw += chunk))
    req.on('end', () => {
      entry.body = new URLSearchParams(raw)
      handle()
    })

    const handle = (): void => {
      const index = mock.overrides.findIndex((o) => o.path === path)
      if (index !== -1) {
        const [override] = mock.overrides.splice(index, 1)
        res.statusCode = override!.status
        res.end(override!.body ?? '')
        return
      }

      if (path === '/signalr/negotiate' && mock.delays.negotiate > 0) {
        setTimeout(reply, mock.delays.negotiate)
      } else if (path === '/signalr/start' && mock.delays.start > 0) {
        setTimeout(reply, mock.delays.start)
      } else reply()
    }

    const reply = (): void => {
      if (path === '/signalr/negotiate') {
        const queued = mock.negotiateQueue.shift()
        res.statusCode = queued === undefined ? mock.negotiate.status : 200
        res.setHeader('Set-Cookie', ['affinity=abc; Path=/; HttpOnly'])
        const body = queued === undefined ? mock.negotiate.body : queued
        res.end(typeof body === 'string' ? body : JSON.stringify(body))
        return
      }

      if (path === '/signalr/start') {
        res.statusCode = mock.start.status
        res.end(mock.start.body)
        return
      }

      if (path === '/signalr/ping') {
        res.statusCode = mock.pingStatus
        res.end('{"Response":"pong"}')
        return
      }

      const isConnect =
        path === '/signalr/connect' || path === '/signalr/reconnect'

      if (isConnect && transport && mock.failTransports.has(transport)) {
        res.statusCode = 500
        res.end()
        return
      }

      if (isConnect && transport === 'serverSentEvents') {
        res.writeHead(200, { 'Content-Type': 'text/event-stream' })
        res.write('data: initialized\n\n')
        const send = (payload: string): void =>
          void res.write(`data: ${payload}\n\n`)
        pushes.add(send)
        const end = (): void => void res.end()
        streams.add(end)
        res.on('close', () => {
          pushes.delete(send)
          streams.delete(end)
        })
        if (path.endsWith('/connect')) {
          send(mock.initRaw ?? JSON.stringify(mock.init))
          for (const frame of mock.frames) send(frame)
        }
        return
      }

      if (
        (isConnect && transport === 'longPolling') ||
        path === '/signalr/poll'
      ) {
        const respond = (payload: string): void => void res.end(payload)
        if (path.endsWith('/connect')) {
          respond(mock.initRaw ?? JSON.stringify(mock.init))
          pollQueue.push(...mock.frames) // delivered by the following polls
        } else if (pollQueue.length > 0) {
          respond(pollQueue.shift()!)
        } else {
          waitingPoll = respond
          res.on('close', () => {
            if (waitingPoll === respond) waitingPoll = undefined
          })
        }
        return
      }

      if (path === '/signalr/send') {
        const data = entry.body.get('data') ?? ''
        res.end(mock.onClientMessage?.(data) ?? '{}')
        return
      }

      res.end('{}')
    }
  })

  server.on('connection', (socket) => {
    connections.add(socket)
    socket.on('close', () => connections.delete(socket))
  })

  server.on('upgrade', (req, socket) => {
    const { path } = record('WS', req)
    socket.on('error', () => {})
    if (mock.failTransports.has('webSockets')) {
      socket.end('HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n')
      return
    }
    const accept = createHash('sha1')
      .update(`${req.headers['sec-websocket-key']}${GUID}`)
      .digest('base64')
    socket.write(
      'HTTP/1.1 101 Switching Protocols\r\n' +
        'Upgrade: websocket\r\nConnection: Upgrade\r\n' +
        `Sec-WebSocket-Accept: ${accept}\r\n\r\n`
    )
    const send = (payload: string): void =>
      void socket.write(textFrame(payload))
    const sendBinary = (payload: string): void => {
      const data = Buffer.from(payload)
      socket.write(Buffer.concat([Buffer.from([0x82, data.length]), data]))
    }
    pushes.add(send)
    binaries.add(sendBinary)
    socket.on('close', () => {
      pushes.delete(send)
      binaries.delete(sendBinary)
    })
    if (path.endsWith('/connect')) {
      send(mock.initRaw ?? JSON.stringify(mock.init))
      for (const frame of mock.frames) send(frame)
    }
    socket.on('data', (chunk: Buffer) => {
      const { opcode, text } = unmask(chunk)
      // Echo a close frame back when the client closes.
      if (opcode === 0x8) return void socket.end(Buffer.from([0x88, 0x00]))
      if (opcode === 0x1) {
        const reply = mock.onClientMessage?.(text)
        if (reply) send(reply)
      }
    })
  })

  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  mock.url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/signalr`
  mock.close = async (): Promise<void> => {
    mock.dropSockets()
    server.closeAllConnections()
    await new Promise<void>((resolve) => server.close(() => resolve()))
  }
  return mock
}
