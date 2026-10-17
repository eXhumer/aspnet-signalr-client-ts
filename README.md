# @exhumer/aspnet-signalr-client

[![CI](https://github.com/eXhumer/aspnet-signalr-client-ts/actions/workflows/ci.yml/badge.svg)](https://github.com/eXhumer/aspnet-signalr-client-ts/actions/workflows/ci.yml)
[![Coverage](https://codecov.io/gh/eXhumer/aspnet-signalr-client-ts/branch/main/graph/badge.svg)](https://codecov.io/gh/eXhumer/aspnet-signalr-client-ts)
[![npm](https://img.shields.io/npm/v/@exhumer/aspnet-signalr-client)](https://www.npmjs.com/package/@exhumer/aspnet-signalr-client)

A SignalR client for Node.js which support ASP.NET but not ASP.NET Core. For ASP.NET Core SignalR support use the offical client from Microsoft or `@exhumer/signalr-client`.

This started as a fork of the original [node-signalr](https://www.npmjs.com/package/node-signalr) project, made to connect to F1's live timing SignalR server, which sits behind a [Google Cloud Load Balancer](https://cloud.google.com/load-balancing/docs/backend-service#generated_cookie_affinity). The load balancer pins a client to a backend with a cookie set during the negotiation phase, so that cookie has to be sent on every later request.

It has since been rewritten on [undici](https://github.com/nodejs/undici) and TypeScript, modelled after Microsoft's own JavaScript and .NET clients for ASP.NET SignalR (see [Protocol](#protocol)). Cookies from the negotiation phase are kept in a [tough-cookie](https://github.com/salesforce/tough-cookie) jar through [@exhumer/undici-cookie-agent](https://github.com/eXhumer/undici-cookie-agent) and sent on every request, WebSocket handshake included, which keeps the load balancer affinity working.

## Install

```bash
$ npm i @exhumer/aspnet-signalr-client
```

## Usage

### Create a SignalR Client Instance

```ts
import { Client } from '@exhumer/aspnet-signalr-client'

const client = new Client('http://localhost:8080/signalr', ['testHub'])
```

### Configuring Client

```ts
import { ProxyAgent } from 'undici'

// custom headers
client.headers['Token'] = 'Tds2dsJk'

// custom query string parameters, added to every request
client.qs['key'] = 'value'

// timeout for a hub method call (ms)
client.callTimeout = 10_000 // default 5000

// delay before a reconnect attempt (ms)
client.reconnectDelayTime = 3000 // default 2000

// timeout for the HTTP requests and for opening a connection (ms)
client.requestTimeout = 2000 // default 5000

// extra time (ms) added to the server's TransportConnectTimeout, the time a
// transport has to deliver the init message before falling back
client.transportConnectTimeout = 0 // default 0

// interval (ms) of the /ping requests that keep the server session alive,
// 0 disables them
client.pingInterval = 300_000 // default 300000

// transports in order of preference, the next one is tried when one fails to
// connect. webSockets is skipped if the server doesn't support it.
client.transports = ['webSockets', 'serverSentEvents', 'longPolling'] // default

// cookies from the negotiate response are kept in a tough-cookie jar and sent
// on every request, pre-fill it to authenticate with existing cookies
await client.cookieJar.setCookie('session=abc', 'http://localhost:8080')

// custom undici dispatcher (proxy, TLS options, ...)
client.dispatcher = new ProxyAgent('http://proxy:8080')
```

### Binding Client Events

```ts
client.on('connected', () => {
  console.log('SignalR client connected.')
})
client.on('reconnecting', (retryCount) => {
  console.log(`SignalR client reconnecting(${retryCount}).`)
})
client.on('disconnected', (reason) => {
  console.log(`SignalR client disconnected(${reason}).`)
})
client.on('connectionSlow', () => {
  console.log('SignalR client connection is slow.')
})
client.on('error', (error) => {
  console.log(`SignalR client connect error: ${error.code}.`)
})
```

`reconnecting` is emitted for every reconnect attempt, `connectionSlow` when no
keep-alive was received for two thirds of the server's keep-alive timeout.

### Binding Hub Method

- Bind callback for receive message

```js
client.connection.hub.on('testHub', 'getMessage', (message) => {
  console.log('receive:', message)
})
```

- Call the hub method and get return values asynchronously

```ts
const message = { user: '', message: '' }

client.connection.hub
  .call('testHub', 'send', message)
  .then((result) => {
    console.log('success:', result)
  })
  .catch((error) => {
    console.log('error:', error)
  })
```

- Invoke the hub method without return values

```ts
const message = { user: '', message: '' }

client.connection.hub.invoke('testHub', 'send', message)
```

### Start Client

```ts
client.start()
```

### End Client

```ts
client.end()
```

## Protocol

The client follows Microsoft's JavaScript and .NET clients for ASP.NET SignalR
(client protocol 2.1, servers speaking 1.4 to 2.1 are accepted):

- `negotiate` is followed through `RedirectUrl` redirects (with their access
  token), `ProtocolVersion` is checked and ASP.NET Core servers are refused.
- Transports are tried in order (WebSockets, server-sent events, long polling)
  and a transport that fails before the init message is replaced by the next
  one. `foreverFrame` is a browser transport and isn't implemented.
- The `start` request is sent once the init message (`S`) arrives and the
  connection is connected once the server answers `started`. Hub messages that
  arrive in between are delivered right before `connected`.
- A lost connection is resumed with `/reconnect` (last message id and groups
  token). Past the server's `DisconnectTimeout` a new connection is negotiated
  instead, and failures while negotiating or starting are retried.
- Long polling retries by itself, as the reference client does: after a failed
  poll it waits 3 seconds, polls `/reconnect`, and raises `connected` again
  after a delay that grows with every failed attempt (or as soon as a poll
  returns). `reconnectDelayTime` applies to the other transports.
- Hub names are sent in lower case, GET requests carry a `_=<timestamp>`
  cache-buster, and an unreadable `/send` response stops the connection.
- Keep-alives are monitored on WebSockets and server-sent events. Long polling
  has none.
- Hub state (`S`) is sent with every invocation and updated from server
  messages and results.

Errors of a hub invocation are `HubError`s (`isHubError` and `data` come from
the server), the other errors are `SignalRError`s with a `code`.

## Powered By

- [undici](https://github.com/nodejs/undici)
- [@exhumer/undici-cookie-agent](https://github.com/eXhumer/undici-cookie-agent)

## License

[MIT](./LICENSE)

## Differences from the reference clients

The client follows Microsoft's ASP.NET SignalR JavaScript client (protocol 2.1)
except for the following.

Deliberate differences:

- Failures of `negotiate` and `start` are retried instead of ending the
  connection, and a connection that was lost for longer than the server's
  `DisconnectTimeout` is renegotiated rather than stopped.
- `reconnecting` is emitted for every attempt, and a lost connection emits
  `disconnected` with `'failed'` before it is retried.
- Hub invocations time out after `callTimeout`.
- Hubs are declared in the constructor (`NO_HUB` is raised without any), there
  are no generated proxies.
- The bearer token is sent in the `Authorization` header on WebSockets and
  server-sent events too (and as `access_token` in their query string).
- Sending before the connection is started rejects the call instead of
  throwing, and errors thrown by event handlers are rethrown on the next tick.
- ASP.NET Core SignalR servers (`availableTransports` in the negotiate
  response) are rejected.

Not implemented:

- The `foreverFrame` transport (Internet Explorer only).
- The jQuery API: `$.connection`, the `/signalr/hubs` proxy script,
  `stateChanged`, `starting`, `sending` and `received` events, `start()` options
  (`waitForPageLoad`, `jsonp`, a single `transport`), `stop(async, notifyServer)`.
- Browser features: CORS and `withCredentials`, JSONP, `webSocketServerUrl`,
  custom JSON parsers, the connection `logging` option.
- Progress callbacks of hub invocations (`P` messages are received and ignored).
- A WebSocket rejected with `401` is not reported as unauthorized: neither
  browsers nor Microsoft's client can see the handshake status, and undici's
  WebSocket does not expose it either.
