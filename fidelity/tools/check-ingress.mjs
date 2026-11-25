// 在無外網的測試容器用實際 CLI 比較同一段輸入；假上游只回固定的工具與文字。
// node check-ingress.mjs <codex> <ws 模組路徑>。只印尺寸／型別，不留對話與憑據。
import assert from 'node:assert/strict'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'
import http from 'node:http'
import { spawn } from 'node:child_process'
import { once } from 'node:events'
import { createRequire } from 'node:module'
import { performance } from 'node:perf_hooks'
import { gunzipSync, zstdDecompressSync } from 'node:zlib'
const [bin, wsModule] = process.argv.slice(2)
const { WebSocketServer } = createRequire(import.meta.url)(wsModule || 'ws')
const prompt = 'Test fixture. ' + 'history-abc123 '.repeat(21000)
for (const useWs of [false, true]) {
  const root = await fs.mkdtemp(path.join(os.tmpdir(), 'kin-ingress-'))
  const wss = new WebSocketServer({ noServer: true, perMessageDeflate: false })
  const rows = []
  let n = 0,
    calls = 0
  function events(msg, transport, encoded, decoded, encoding, wire) {
    const id = 'resp_fixture_' + ++n,
      model = msg.model
    const warm = msg.generate === false
    const tool = !warm && ++calls === 1
    const item = tool
      ? { type: 'function_call', id: 'fc_fixture', call_id: 'call_fixture', name: 'fixture_noop', arguments: '{}' }
      : {
          type: 'message',
          id: 'msg_fixture',
          role: 'assistant',
          content: [{ type: 'output_text', text: 'ok', annotations: [] }],
        }
    rows.push({
      transport,
      encoding,
      encoded_bytes: encoded,
      decoded_bytes: decoded,
      ws_frame_bytes: wire,
      previous: !!msg.previous_response_id,
      history: JSON.stringify(msg.input).includes('history-abc123'),
      warmup: warm,
      items: msg.input?.length,
    })
    const response = {
      id,
      model,
      status: 'completed',
      output: warm ? [] : [item],
      usage: { input_tokens: 10, output_tokens: 1, total_tokens: 11 },
    }
    return [
      { type: 'response.created', response: { ...response, status: 'in_progress', output: [] } },
      ...(warm
        ? []
        : [
            { type: 'response.output_item.added', output_index: 0, item: { ...item, status: 'in_progress' } },
            ...(tool
              ? []
              : [
                  {
                    type: 'response.output_text.delta',
                    output_index: 0,
                    content_index: 0,
                    item_id: item.id,
                    delta: 'ok',
                  },
                ]),
            { type: 'response.output_item.done', output_index: 0, item },
          ]),
      { type: 'response.completed', response },
    ]
  }
  const server = http.createServer(async (req, res) => {
    if (!req.url.endsWith('/responses')) {
      res.writeHead(404).end()
      return
    }
    const parts = []
    for await (const c of req) parts.push(c)
    const raw = Buffer.concat(parts),
      enc = req.headers['content-encoding'] || 'identity'
    const plain = enc === 'zstd' ? zstdDecompressSync(raw) : enc === 'gzip' ? gunzipSync(raw) : raw
    const answer = events(JSON.parse(plain), 'http', raw.length, plain.length, enc)
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    res.end(answer.map((e) => 'event: ' + e.type + '\ndata: ' + JSON.stringify(e) + '\n\n').join(''))
  })
  server.on('upgrade', (req, sock, head) =>
    wss.handleUpgrade(req, sock, head, (ws) => {
      let last = sock.bytesRead
      ws.on('message', (b) => {
        const wire = sock.bytesRead - last
        last = sock.bytesRead
        for (const e of events(JSON.parse(b), 'websocket', b.length, b.length, 'identity', wire))
          ws.send(JSON.stringify(e))
      })
    }),
  )
  const started = performance.now()
  try {
    server.listen(0, '127.0.0.1')
    await once(server, 'listening')
    await fs.writeFile(
      path.join(root, 'config.toml'),
      `model_provider="fixture"
model="gpt-5.5"
model_reasoning_effort="high"
check_for_update_on_startup=false
[model_providers.fixture]
name="fixture"
base_url="http://127.0.0.1:${server.address().port}/v1"
wire_api="responses"
experimental_bearer_token="fixture-only"
supports_websockets=${useWs}
`,
    )
    const cli = spawn(bin, ['exec', '--ephemeral', '--skip-git-repo-check', '-'], {
      cwd: root,
      env: { PATH: process.env.PATH, HOME: root, CODEX_HOME: root, TERM: 'dumb' },
      stdio: ['pipe', 'pipe', 'pipe'],
    })
    let stderr = ''
    cli.stderr.on('data', (b) => (stderr = (stderr + b).slice(-1500)))
    cli.stdout.resume()
    const timer = setTimeout(() => cli.kill('SIGKILL'), 30000)
    timer.unref()
    cli.stdin.end(prompt)
    const [code] = await once(cli, 'exit')
    clearTimeout(timer)
    assert.equal(code, 0, stderr)
    assert.equal(calls, 2, JSON.stringify(rows))
    const gen = rows.filter((r) => !r.warmup)
    assert.ok(gen[0].history)
    assert.equal(gen[1].transport, useWs ? 'websocket' : 'http')
    assert.equal(gen[1].history, !useWs)
    assert.equal(gen[1].previous, useWs)
    console.log(JSON.stringify({ websocket: useWs, elapsed_ms: Math.round(performance.now() - started), rows }))
  } finally {
    for (const c of wss.clients) c.terminate()
    wss.close()
    server.closeAllConnections()
    server.close()
    await fs.rm(root, { recursive: true, force: true })
  }
}
