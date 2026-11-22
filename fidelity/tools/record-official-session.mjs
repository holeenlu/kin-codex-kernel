// Records the official Codex CLI end to end against a TLS-terminating fake ChatGPT backend.
//
// Runs as root inside a `--network none` container where chatgpt.com / auth.openai.com
// resolve to 127.0.0.1. Every TCP connection on :443 first has its ClientHello parsed
// (tls-capture.mjs from vm2api, mounted at /repo), then is handed to an HTTPS server
// that serves the few endpoints the CLI needs to reach the Responses WebSocket.
//
// The capture CA is installed into the system trust store paths only; no CA-related
// environment variable is set, so the CLI keeps its production TLS code paths.
//
// Modes: `exec` runs `codex exec <prompt>`; `tui` runs the interactive `codex <prompt>`
// under a pseudo-terminal (util-linux `script`), answers the terminal queries the TUI
// sends, and quits with Ctrl-C once the turn has been answered.
//
// REC_REFRESH records the token refresh (POST auth.openai.com/oauth/token):
//   expiring      the access token expires in 60 s, inside the CLI's 5-minute refresh window;
//   unauthorized  the first Responses request answers 401 (use with REC_WEBSOCKET=reject);
//                 the CLI reloads auth.json and retries without refreshing;
//   unauthorized2 the first two answer 401, so the CLI's recovery goes on to refresh.
//
// REC_TOOL records the requests of tools the CLI runs itself when the model calls them:
//   search     the first generating turn answers a call to web.run (POST /codex/alpha/search);
//   imagegen   a call to image_gen.imagegen (POST /codex/images/generations);
//   imageedit  the same with a referenced image on disk (POST /codex/images/edits).
// The turn that carries the tool output is answered with text, as usual.
//
// REC_COOKIES=1 adds Set-Cookie to every chatgpt.com answer (WebSocket handshake included):
// allowlisted infrastructure cookies, a session cookie and an unlisted one, to record which
// cookies the CLI keeps and where the Cookie header goes.
//
//   node record-official-session.mjs <codex-bin> <certs-dir> <out.json> [exec|tui] [prompt]
import { spawn } from 'node:child_process'
import crypto from 'node:crypto'
import fs from 'node:fs'
import https from 'node:https'
import net from 'node:net'
import zlib from 'node:zlib'
import { parseClientHello, summarize } from '/repo/test/kernel-contract/tls-capture.mjs'

const [, , codexBin, certsDir, outFile, mode = 'exec', prompt = 'ping'] = process.argv
if (!['exec', 'tui'].includes(mode)) throw new Error(`unknown mode ${mode}`)
const ca = fs.readFileSync(`${certsDir}/ca.pem`)
for (const target of [
  '/etc/ssl/certs/ca-certificates.crt',
  '/etc/ssl/cert.pem',
  '/usr/local/ssl/cert.pem',
  '/usr/lib/ssl/cert.pem',
  '/etc/pki/tls/certs/ca-bundle.crt',
]) {
  fs.mkdirSync(target.slice(0, target.lastIndexOf('/')), { recursive: true })
  fs.writeFileSync(target, ca)
}

const ACCOUNT_ID = 'acct-contract'
const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64url')
const auth = {
  'https://api.openai.com/auth': {
    chatgpt_account_id: ACCOUNT_ID,
    chatgpt_plan_type: 'pro',
    chatgpt_user_id: 'user-contract',
  },
}
const jwt = (claims) => `${b64({ alg: 'RS256', typ: 'JWT' })}.${b64(claims)}.c2ln`
const connections = new Map()
const requests = []
const wsMessages = []
let nextConn = 1
let cookieRound = 0
const SET_COOKIES = () => {
  cookieRound += 1
  return [
    `__cf_bm=cfbm-${cookieRound}; Path=/; Domain=.chatgpt.com; Secure; HttpOnly; SameSite=None`,
    `__oailb=lb-${cookieRound}; Path=/; Secure; HttpOnly; SameSite=Lax`,
    'cf_clearance=clearance-1; Path=/; Domain=.chatgpt.com; Secure; HttpOnly',
    '__Secure-next-auth.session-token=session-secret; Path=/; Secure; HttpOnly',
    'oai-did=device-cookie; Path=/; Secure',
  ]
}
const withCookies = (host, headers) =>
  process.env.REC_COOKIES === '1' && String(host || '').endsWith('chatgpt.com')
    ? { ...headers, 'set-cookie': SET_COOKIES() }
    : headers
let refreshes = 0
let unauthorizedSent = 0
// Set when a generating turn (not a prewarm) has been answered.
let turnAnsweredAt = 0
const allConns = []

function decodeBody(buf, encoding) {
  try {
    if (encoding === 'zstd') buf = zlib.zstdDecompressSync(buf)
    else if (encoding === 'gzip') buf = zlib.gunzipSync(buf)
    else if (encoding === 'br') buf = zlib.brotliDecompressSync(buf)
  } catch (error) {
    return { undecodable: String(error.message || error), length: buf.length }
  }
  const text = buf.toString('utf8')
  try {
    return text ? JSON.parse(text) : null
  } catch {
    return text.length > 4000 ? `${text.slice(0, 4000)}…(${text.length})` : text
  }
}

const sse = (event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`

// A 1x1 PNG: the CLI decodes and saves what the image endpoints return.
const PNG_1X1 = 'iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8DwHwAFBQIAX8jx0gAAAABJRU5ErkJggg=='
let toolCalled = false

function toolItem() {
  const call = { type: 'function_call', id: 'fc_capture_1', status: 'completed', call_id: 'call_capture_1' }
  switch (process.env.REC_TOOL) {
    case 'search':
      return { ...call, name: 'run', namespace: 'web', arguments: JSON.stringify({ search_query: [{ q: 'contract test' }] }) }
    case 'imagegen':
      return { ...call, name: 'imagegen', namespace: 'image_gen', arguments: JSON.stringify({ prompt: 'a red square' }) }
    case 'imageedit':
      return {
        ...call,
        name: 'imagegen',
        namespace: 'image_gen',
        arguments: JSON.stringify({ prompt: 'make it blue', referenced_image_paths: [`${work}/input.png`] }),
      }
    default:
      return null
  }
}

/** Events for one generating turn: a tool call first (REC_TOOL), text once the tool output is back. */
function answerTurn(model, input) {
  const item = !toolCalled && !JSON.stringify(input ?? '').includes('function_call_output') ? toolItem() : null
  if (!item) {
    turnAnsweredAt ||= Date.now()
    return turnEvents(model)
  }
  toolCalled = true
  const id = 'resp_capture_tool'
  return [
    { type: 'response.created', response: { id, model, status: 'in_progress', output: [] } },
    { type: 'response.output_item.added', output_index: 0, item: { ...item, status: 'in_progress' } },
    { type: 'response.output_item.done', output_index: 0, item },
    {
      type: 'response.completed',
      response: {
        id,
        model,
        status: 'completed',
        output: [item],
        usage: { input_tokens: 11, input_tokens_details: { cached_tokens: 0 }, output_tokens: 5, output_tokens_details: { reasoning_tokens: 0 }, total_tokens: 16 },
      },
    },
  ]
}

function turnEvents(model) {
  const id = 'resp_capture_1'
  const item = {
    type: 'message',
    role: 'assistant',
    id: 'msg_capture_1',
    status: 'completed',
    content: [{ type: 'output_text', text: 'pong', annotations: [] }],
  }
  return [
    { type: 'response.created', response: { id, model, status: 'in_progress', output: [] } },
    { type: 'response.output_item.added', output_index: 0, item: { ...item, status: 'in_progress', content: [] } },
    { type: 'response.output_text.delta', output_index: 0, content_index: 0, item_id: item.id, delta: 'pong' },
    { type: 'response.output_item.done', output_index: 0, item },
    {
      type: 'response.completed',
      response: {
        id,
        model,
        status: 'completed',
        output: [item],
        usage: {
          input_tokens: 11,
          input_tokens_details: { cached_tokens: 0 },
          output_tokens: 2,
          output_tokens_details: { reasoning_tokens: 0 },
          total_tokens: 13,
        },
      },
    },
  ]
}

const accountsCheck = {
  accounts: [
    {
      id: ACCOUNT_ID,
      can_access_with_session: true,
      is_deactivated: false,
      plan_type: 'pro',
      workspace_backend_origin: 'NO_CONSTRAINT',
      account_routing_override: 'NO_CONSTRAINT',
      name: null,
      structure: 'personal',
    },
  ],
  account_ordering: [ACCOUNT_ID],
  default_account_id: ACCOUNT_ID,
}

function connOf(socket) {
  return connections.get(`127.0.0.1:${socket.remotePort}`)
}

const server = https.createServer(
  {
    key: fs.readFileSync(`${certsDir}/leaf.key`),
    cert: Buffer.concat([fs.readFileSync(`${certsDir}/leaf.pem`), ca]),
    ALPNProtocols: ['http/1.1'],
  },
  async (req, res) => {
    const chunks = []
    for await (const chunk of req) chunks.push(chunk)
    const conn = connOf(req.socket)
    if (conn && conn.kind === 'none') conn.kind = 'http'
    const entry = {
      conn: conn?.id ?? null,
      at: Date.now(),
      method: req.method,
      host: req.headers.host,
      url: req.url,
      httpVersion: req.httpVersion,
      rawHeaders: req.rawHeaders,
      body: decodeBody(Buffer.concat(chunks), String(req.headers['content-encoding'] || '')),
    }
    requests.push(entry)
    const url = new URL(req.url, 'https://chatgpt.com')
    if (req.method === 'POST' && url.pathname === '/oauth/token') {
      entry.answer = 200
      const issued = Math.floor(Date.now() / 1000)
      refreshes += 1
      res.writeHead(200, { 'content-type': 'application/json' })
      return res.end(
        JSON.stringify({
          id_token: jwt({ email: 'contract@example.invalid', exp: issued + 3600, iat: issued, ...auth }),
          access_token: jwt({ exp: issued + 3600, iat: issued, ...auth }),
          refresh_token: `rt-contract-refreshed-${refreshes}`,
          expires_in: 3600,
          token_type: 'Bearer',
          scope: 'openid profile email offline_access',
        }),
      )
    }
    const unauthorizedPlanned = { unauthorized: 1, unauthorized2: 2 }[process.env.REC_REFRESH] || 0
    if (unauthorizedSent < unauthorizedPlanned && req.method === 'POST' && url.pathname.endsWith('/codex/responses')) {
      unauthorizedSent += 1
      entry.answer = 401
      res.writeHead(401, { 'content-type': 'application/json' })
      return res.end(JSON.stringify({ detail: 'Unauthorized' }))
    }
    if (req.method === 'POST' && url.pathname.endsWith('/codex/alpha/search')) {
      entry.answer = 200
      res.writeHead(200, withCookies(req.headers.host, { 'content-type': 'application/json' }))
      return res.end(JSON.stringify({ id: 'search_capture_1', output: [] }))
    }
    if (req.method === 'POST' && /\/codex\/images\/(generations|edits)$/.test(url.pathname)) {
      entry.answer = 200
      res.writeHead(200, withCookies(req.headers.host, { 'content-type': 'application/json' }))
      return res.end(
        JSON.stringify({
          created: Math.floor(Date.now() / 1000),
          background: 'opaque',
          data: [{ b64_json: PNG_1X1 }],
          output_format: 'png',
          quality: 'medium',
          size: '1024x1024',
          usage: { input_tokens: 10, output_tokens: 20, total_tokens: 30 },
        }),
      )
    }
    if (url.pathname.endsWith('/wham/accounts/check') || url.pathname.endsWith('/api/codex/accounts/check')) {
      entry.answer = 200
      res.writeHead(200, withCookies(req.headers.host, { 'content-type': 'application/json' }))
      return res.end(JSON.stringify(accountsCheck))
    }
    if (req.method === 'POST' && url.pathname.endsWith('/codex/responses')) {
      entry.answer = 200
      res.writeHead(200, withCookies(req.headers.host, { 'content-type': 'text/event-stream', 'cache-control': 'no-cache' }))
      for (const event of answerTurn(entry.body?.model || 'gpt-5.5', entry.body?.input)) res.write(sse(event))
      return res.end()
    }
    entry.answer = 404
    res.writeHead(404, withCookies(req.headers.host, { 'content-type': 'application/json' }))
    res.end(JSON.stringify({ detail: 'Not Found' }))
  },
)

function wsFrame(opcode, payload) {
  const data = Buffer.isBuffer(payload) ? payload : Buffer.from(payload)
  let header
  if (data.length < 126) header = Buffer.from([0x80 | opcode, data.length])
  else if (data.length < 65536) {
    header = Buffer.alloc(4)
    header[0] = 0x80 | opcode
    header[1] = 126
    header.writeUInt16BE(data.length, 2)
  } else {
    header = Buffer.alloc(10)
    header[0] = 0x80 | opcode
    header[1] = 127
    header.writeBigUInt64BE(BigInt(data.length), 2)
  }
  return Buffer.concat([header, data])
}

server.on('upgrade', (req, socket, head) => {
  const conn = connOf(socket)
  if (conn) conn.kind = 'websocket'
  const entry = {
    conn: conn?.id ?? null,
    at: Date.now(),
    method: req.method,
    host: req.headers.host,
    url: req.url,
    httpVersion: req.httpVersion,
    rawHeaders: req.rawHeaders,
    websocket: true,
  }
  requests.push(entry)
  // REC_WEBSOCKET=reject answers 426, which makes the official client switch the session
  // to HTTP (core/src/client.rs: UPGRADE_REQUIRED -> try_switch_fallback_transport).
  if (process.env.REC_WEBSOCKET === 'reject') {
    entry.answer = 426
    socket.end('HTTP/1.1 426 Upgrade Required\r\nContent-Length: 0\r\nConnection: close\r\n\r\n')
    return
  }
  const key = req.headers['sec-websocket-key']
  const accept = crypto
    .createHash('sha1')
    .update(`${key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11`)
    .digest('base64')
  socket.write(
    `HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\n${process.env.REC_COOKIES === '1' ? SET_COOKIES().map((c) => `Set-Cookie: ${c}\r\n`).join('') : ''}\r\n`,
  )
  entry.answer = 101
  let buf = head && head.length ? Buffer.from(head) : Buffer.alloc(0)
  let fragments = []
  let fragmentOpcode = 0
  socket.on('error', () => {})
  socket.on('data', (chunk) => {
    buf = Buffer.concat([buf, chunk])
    for (;;) {
      if (buf.length < 2) return
      const fin = (buf[0] & 0x80) !== 0
      const opcode = buf[0] & 0x0f
      const masked = (buf[1] & 0x80) !== 0
      let len = buf[1] & 0x7f
      let at = 2
      if (len === 126) {
        if (buf.length < 4) return
        len = buf.readUInt16BE(2)
        at = 4
      } else if (len === 127) {
        if (buf.length < 10) return
        len = Number(buf.readBigUInt64BE(2))
        at = 10
      }
      const maskAt = at
      if (masked) at += 4
      if (buf.length < at + len) return
      let payload = Buffer.from(buf.subarray(at, at + len))
      if (masked) for (let i = 0; i < payload.length; i += 1) payload[i] ^= buf[maskAt + (i % 4)]
      const rsv1 = (buf[0] & 0x40) !== 0
      buf = buf.subarray(at + len)
      if (opcode === 0x8) {
        wsMessages.push({ conn: conn?.id ?? null, at: Date.now(), type: 'close', payload: payload.toString('hex') })
        socket.end(wsFrame(0x8, payload.subarray(0, 2)))
        return
      }
      if (opcode === 0x9) {
        wsMessages.push({ conn: conn?.id ?? null, at: Date.now(), type: 'ping', length: payload.length })
        socket.write(wsFrame(0xa, payload))
        continue
      }
      if (opcode === 0xa) continue
      if (opcode !== 0) fragmentOpcode = opcode
      fragments.push(payload)
      if (!fin) continue
      const whole = Buffer.concat(fragments)
      fragments = []
      let message = whole.toString('utf8')
      try {
        message = JSON.parse(message)
      } catch {}
      wsMessages.push({ conn: conn?.id ?? null, at: Date.now(), opcode: fragmentOpcode, rsv1, message })
      if (message && typeof message === 'object' && message.type === 'response.create') {
        const events =
          message.generate === false ? turnEvents(message.model || 'gpt-5.5') : answerTurn(message.model || 'gpt-5.5', message.input)
        for (const event of events) socket.write(wsFrame(0x1, JSON.stringify(event)))
      }
    }
  })
})

// The HTTPS server listens on an internal port; the front listener parses each
// ClientHello and then relays the raw bytes unchanged, so the handshake is untouched.
await new Promise((resolve) => server.listen(8443, '127.0.0.1', resolve))
const front = net.createServer((socket) => {
  socket.on('error', () => {})
  const conn = { id: nextConn++, at: Date.now(), kind: 'none', hello: null }
  allConns.push(conn)
  let buf = Buffer.alloc(0)
  const onData = (chunk) => {
    buf = Buffer.concat([buf, chunk])
    if (buf.length < 5) return
    const need = 5 + buf.readUInt16BE(3)
    if (buf.length < need) return
    socket.removeListener('data', onData)
    socket.pause()
    try {
      conn.hello = summarize(parseClientHello(buf.subarray(0, need)))
    } catch (error) {
      conn.hello = { error: String(error.message || error) }
    }
    const inner = net.connect(8443, '127.0.0.1', () => {
      connections.set(`127.0.0.1:${inner.localPort}`, conn)
      inner.write(buf)
      socket.pipe(inner)
      inner.pipe(socket)
      socket.resume()
    })
    inner.on('error', () => socket.destroy())
    socket.on('close', () => inner.destroy())
    inner.on('close', () => socket.destroy())
  }
  socket.on('data', onData)
})
await new Promise((resolve) => front.listen(443, '127.0.0.1', resolve))

const now = Math.floor(Date.now() / 1000)
const home = fs.mkdtempSync('/tmp/codex-home-')
fs.writeFileSync(
  `${home}/auth.json`,
  JSON.stringify({
    OPENAI_API_KEY: null,
    tokens: {
      id_token: jwt({ email: 'contract@example.invalid', exp: now + 3600, iat: now, ...auth }),
      access_token: jwt({ exp: now + (process.env.REC_REFRESH === 'expiring' ? 60 : 3600), iat: now, ...auth }),
      refresh_token: 'rt-contract',
      account_id: ACCOUNT_ID,
    },
    last_refresh: new Date(now * 1000).toISOString(),
  }),
)
const work = fs.mkdtempSync('/tmp/codex-work-')
if (process.env.REC_TOOL === 'imageedit') fs.writeFileSync(`${work}/input.png`, Buffer.from(PNG_1X1, 'base64'))
// Trusting the working directory up front skips the TUI's trust prompt.
fs.writeFileSync(
  `${home}/config.toml`,
  `check_for_update_on_startup = false\n\n[projects.${JSON.stringify(work)}]\ntrust_level = "trusted"\n`,
)
// Terminal variables are passed through so the terminal segment of the User-Agent can
// be recorded for a given terminal (docker -e TERM=... -e TERM_PROGRAM=...).
const terminalEnv = Object.fromEntries(
  ['TERM', 'TERM_PROGRAM', 'TERM_PROGRAM_VERSION', 'VTE_VERSION']
    .filter((name) => process.env[name])
    .map((name) => [name, process.env[name]]),
)
const env = { PATH: process.env.PATH, HOME: home, CODEX_HOME: home, LANG: 'C.UTF-8', RUST_LOG: 'warn', ...terminalEnv }
const quote = (value) => `'${String(value).replaceAll("'", "'\\''")}'`
const child =
  mode === 'exec'
    ? spawn(codexBin, ['exec', '--skip-git-repo-check', prompt], { cwd: work, env, stdio: ['ignore', 'pipe', 'pipe'] })
    : spawn(
        'script',
        ['-qfec', `stty rows 40 cols 120; exec ${quote(codexBin)} ${quote(prompt)}`, '/dev/null'],
        { cwd: work, env: { TERM: 'xterm-256color', ...env }, stdio: ['pipe', 'pipe', 'pipe'] },
      )
let out = ''
const answered = []
let pending = ''
// Minimal terminal: answer the queries the TUI blocks on (cursor position, device
// attributes, default colours). Sequences may straddle chunks, so keep a short tail.
const QUERIES = [
  ['\u001b[6n', '\u001b[1;1R'],
  ['\u001b[c', '\u001b[?62;22c'],
  ['\u001b[0c', '\u001b[?62;22c'],
  ['\u001b]10;?', '\u001b]10;rgb:d0d0/d0d0/d0d0\u001b\\'],
  ['\u001b]11;?', '\u001b]11;rgb:1c1c/1c1c/1c1c\u001b\\'],
]
child.stdout.on('data', (chunk) => {
  const text = chunk.toString('latin1')
  out += text
  if (mode !== 'tui') return
  pending += text
  for (const [query, reply] of QUERIES) {
    let at = pending.indexOf(query)
    while (at !== -1) {
      child.stdin.write(reply)
      answered.push(JSON.stringify(query))
      pending = pending.slice(0, at) + pending.slice(at + query.length)
      at = pending.indexOf(query)
    }
  }
  pending = pending.slice(-16)
})
child.stderr.on('data', (c) => {
  out += c
})
let quitAt = 0
const quitter =
  mode === 'tui'
    ? setInterval(() => {
        if (!turnAnsweredAt || Date.now() - turnAnsweredAt < 8000) return
        if (!quitAt) quitAt = Date.now()
        // Ctrl-C twice is the TUI's quit gesture; repeat until it exits.
        child.stdin.write('\u0003')
        setTimeout(() => child.stdin.write('\u0003'), 300)
      }, 2000)
    : null
const exited = await new Promise((resolve) => {
  const timer = setTimeout(() => {
    child.kill('SIGKILL')
    resolve('timeout')
  }, 150_000)
  child.on('exit', (code, signal) => {
    clearTimeout(timer)
    resolve({ code, signal })
  })
})
if (quitter) clearInterval(quitter)
await new Promise((resolve) => setTimeout(resolve, 500))
front.close()
server.close()
const front_all = allConns
const screen = out
  .replace(/\u001b\[[0-9;?]*[ -/]*[@-~]/g, '')
  .replace(/\u001b\][^\u0007\u001b]*(\u0007|\u001b\\)/g, '')
  .replace(/\u001b[()][0-9A-Za-z]/g, '')
  .replace(/[\u0000-\u0008\u000b-\u001f]/g, '')
const result = {
  mode,
  exited,
  turn_answered: Boolean(turnAnsweredAt),
  quit_after_ms: quitAt && turnAnsweredAt ? quitAt - turnAnsweredAt : null,
  terminal_queries_answered: answered,
  connections_total: nextConn - 1,
  output_tail: (mode === 'tui' ? screen : out).slice(-6000),
  connections: allConns,
  requests,
  ws_messages: wsMessages,
  refresh_mode: process.env.REC_REFRESH || null,
  auth_json_after: (() => {
    try {
      return JSON.parse(fs.readFileSync(`${home}/auth.json`, 'utf8'))
    } catch {
      return null
    }
  })(),
}
fs.writeFileSync(outFile, JSON.stringify(result, null, 2))
const kinds = {}
for (const c of front_all) kinds[`${c.kind} ${c.hello?.server_name} ${c.hello?.ja4}`] = (kinds[`${c.kind} ${c.hello?.server_name} ${c.hello?.ja4}`] || 0) + 1
console.log(JSON.stringify({ exited, kinds, requests: requests.map((r) => `${r.method} ${r.url} -> ${r.answer}`) }, null, 1))
process.exit(0)
