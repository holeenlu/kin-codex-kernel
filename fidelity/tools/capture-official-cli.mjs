// Runs the official Codex CLI against a local ClientHello capture on :443.
// chatgpt.com / auth.openai.com resolve to 127.0.0.1 (docker --add-host); no network.
// Needs vm2api mounted at /repo for tls-capture.mjs. Usage: see fidelity/README.md.
//   node capture-official-cli.mjs <codex-bin> <out.json> <fresh|expired|stale>
import { spawn } from 'node:child_process'
import fs from 'node:fs'
import { startTlsCapture } from '/repo/test/kernel-contract/tls-capture.mjs'

const [, , codexBin, outFile, mode] = process.argv
const b64 = (o) => Buffer.from(JSON.stringify(o)).toString('base64url')
const now = Math.floor(Date.now() / 1000)
const auth = { 'https://api.openai.com/auth': { chatgpt_account_id: 'acct-contract', chatgpt_plan_type: 'pro', chatgpt_user_id: 'user-contract' } }
const jwt = (claims) => `${b64({ alg: 'RS256', typ: 'JWT' })}.${b64(claims)}.c2ln`
const home = fs.mkdtempSync('/tmp/codex-home-')
fs.writeFileSync(`${home}/auth.json`, JSON.stringify({
  OPENAI_API_KEY: null,
  tokens: {
    id_token: jwt({ email: 'contract@example.invalid', exp: now + 3600, iat: now, ...auth }),
    access_token: jwt({ exp: mode === 'expired' ? now - 60 : now + 3600, iat: now - 7200, ...auth }),
    refresh_token: 'rt-contract',
    account_id: 'acct-contract',
  },
  last_refresh: new Date((mode === 'stale' ? now - 40 * 86400 : now) * 1000).toISOString(),
}))
fs.writeFileSync(`${home}/config.toml`, 'check_for_update_on_startup = false\n')
const capture = await startTlsCapture({ port: 443 })
const work = fs.mkdtempSync('/tmp/codex-work-')
const child = spawn(codexBin, ['exec', '--skip-git-repo-check', 'ping'], {
  cwd: work,
  env: { PATH: process.env.PATH, HOME: home, CODEX_HOME: home, LANG: 'C.UTF-8', RUST_LOG: 'warn' },
  stdio: ['ignore', 'pipe', 'pipe'],
})
let out = ''
child.stdout.on('data', (c) => (out += c))
child.stderr.on('data', (c) => (out += c))
const exited = await new Promise((resolve) => {
  const timer = setTimeout(() => { child.kill('SIGKILL'); resolve('timeout') }, 60_000)
  child.on('exit', (code, signal) => { clearTimeout(timer); resolve({ code, signal }) })
})
await capture.close()
fs.writeFileSync(outFile, JSON.stringify({ mode, exited, output_tail: out.slice(-3000), captures: capture.captures }, null, 2))
console.log(`captures=${capture.captures.length} exited=${JSON.stringify(exited)}`)
