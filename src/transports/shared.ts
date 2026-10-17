import { request } from 'undici'
import type { TransportHost, TransportName } from './types.js'

export const FORM_CONTENT_TYPE =
  'application/x-www-form-urlencoded; charset=UTF-8'

export class HttpStatusError extends Error {
  constructor(readonly statusCode: number) {
    super(`Server responded with status code ${statusCode}.`)
    this.name = 'HttpStatusError'
  }
}

/**
 * Client-to-server messages for the transports that can't use a duplex
 * channel (server-sent events, long polling).
 */
export async function postSend(
  host: TransportHost,
  transport: TransportName,
  payload: string
): Promise<string> {
  const res = await request(host.sendUrl(transport), {
    method: 'POST',
    headers: { ...host.headers, 'Content-Type': FORM_CONTENT_TYPE },
    body: new URLSearchParams({ data: payload }).toString(),
    dispatcher: host.dispatcher,
    signal: AbortSignal.timeout(host.requestTimeout)
  })
  const body = await res.body.text()
  if (res.statusCode !== 200) throw new HttpStatusError(res.statusCode)
  return body
}
