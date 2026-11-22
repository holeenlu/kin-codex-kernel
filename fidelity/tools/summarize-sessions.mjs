// Summarizes one or more record-official-session.mjs runs into the handshake facts the
// baseline needs (per transport: fingerprints, connection counts, extension-order
// variation) and writes one redacted session (requests and WebSocket messages).
//   node summarize-sessions.mjs <out-tls-sessions.json> <out-session.json> <run.json>...
import fs from 'node:fs'

const FIELDS = [
  'legacy_version',
  'session_id_length',
  'ciphers',
  'extensions',
  'server_name',
  'alpn',
  'supported_versions',
  'supported_groups',
  'key_share_groups',
  'signature_algorithms',
  'psk_modes',
  'ec_point_formats',
  'grease',
  'ja4',
]
const [, , outTls, outSession, ...runs] = process.argv
const pick = (hello) => {
  const withSet = { ...hello, extensions: [...hello.extension_order].sort() }
  return Object.fromEntries(FIELDS.map((field) => [field, withSet[field]]))
}
const transports = {}
const summary = []
for (const file of runs) {
  const run = JSON.parse(fs.readFileSync(file, 'utf8'))
  const counts = {}
  const ordersThisRun = {}
  for (const conn of run.connections) {
    if (!conn.hello || conn.hello.error) continue
    // A connection that never carried a request is still the HTTP client's handshake.
    const kind = conn.kind === 'websocket' ? 'websocket' : 'http'
    counts[kind] = (counts[kind] || 0) + 1
    const t = (transports[kind] ||= { connections: 0, orders: new Set(), fingerprints: new Map() })
    t.connections += 1
    t.orders.add(conn.hello.extension_order.join(','))
    ;(ordersThisRun[kind] ||= new Set()).add(conn.hello.extension_order.join(','))
    const print = pick(conn.hello)
    const key = JSON.stringify(print)
    const entry = t.fingerprints.get(key) || { connections: 0, ...print }
    entry.connections += 1
    t.fingerprints.set(key, entry)
  }
  summary.push({
    file: file.split('/').pop(),
    exited: run.exited,
    connections: counts,
    distinct_extension_orders: Object.fromEntries(Object.entries(ordersThisRun).map(([k, v]) => [k, v.size])),
  })
}
const out = {
  method: 'record-official-session.mjs: TLS terminated by a fake backend trusted through the system store only',
  runs: summary,
  transports: Object.fromEntries(
    Object.entries(transports).map(([kind, t]) => [
      kind,
      { connections: t.connections, distinct_extension_orders: t.orders.size, fingerprints: [...t.fingerprints.values()] },
    ]),
  ),
}
fs.writeFileSync(outTls, `${JSON.stringify(out, null, 2)}\n`)

const first = JSON.parse(fs.readFileSync(runs[0], 'utf8'))
const redactHeaders = (raw) => {
  const pairs = []
  for (let i = 0; i < raw.length; i += 2) {
    const name = raw[i]
    const value = /^(authorization|cookie)$/i.test(name) ? `<redacted ${String(raw[i + 1]).split(' ')[0]}>` : raw[i + 1]
    pairs.push([name, value])
  }
  return pairs
}
fs.writeFileSync(
  outSession,
  `${JSON.stringify(
    {
      source: runs[0].split('/').pop(),
      exited: first.exited,
      requests: first.requests.map((request) => ({
        conn: request.conn,
        transport: first.connections.find((conn) => conn.id === request.conn)?.kind ?? null,
        method: request.method,
        host: request.host,
        url: request.url,
        http_version: request.httpVersion,
        headers: redactHeaders(request.rawHeaders),
        answer: request.answer,
        ...(request.websocket ? { websocket: true } : { body: request.body }),
      })),
      ws_messages: first.ws_messages,
    },
    null,
    2,
  )}\n`,
)
console.log(JSON.stringify(summary), Object.keys(out.transports))
