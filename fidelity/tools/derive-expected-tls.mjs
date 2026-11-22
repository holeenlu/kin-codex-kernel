// Derives the contract-test TLS baseline (expected-tls.json) from the capture-only
// recording (tls.json, capture-official-cli.mjs) and the TLS-terminated sessions
// (tls-sessions-<exec|tui>.json, summarize-sessions.mjs). Only the identifying fields are kept.
//   node derive-expected-tls.mjs <tls.json> <tls-sessions-exec.json> <expected-tls.json>
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
const [, , captureOnly, sessions, output] = process.argv
const keep = (print) => Object.fromEntries(FIELDS.map((field) => [field, print[field]]))
const fingerprints = {}
const add = (transport, print) => {
  const hosts = (fingerprints[transport] ||= {})
  const list = (hosts[print.server_name] ||= [])
  const kept = keep(print)
  if (!list.some((item) => JSON.stringify(item) === JSON.stringify(kept))) list.push(kept)
}
const varies = {}

// Capture-only runs (capture-official-cli.mjs): every connection is the HTTP client.
const capture = JSON.parse(fs.readFileSync(captureOnly, 'utf8'))
for (const run of Object.values(capture.runs)) {
  varies.http ||= run.distinct_extension_orders > 1
  for (const print of run.fingerprints) add('http', print)
}

// TLS-terminated sessions (record-official-session.mjs): HTTP and WebSocket separated.
const recorded = JSON.parse(fs.readFileSync(sessions, 'utf8'))
for (const [transport, summary] of Object.entries(recorded.transports)) {
  for (const print of summary.fingerprints) add(transport, print)
  // Variation shows within one run's connections, or across connections of a transport
  // whose fingerprint never changed.
  const perRun = recorded.runs.map((run) => run.distinct_extension_orders[transport] || 0)
  varies[transport] ||= perRun.some((count) => count > 1) || summary.distinct_extension_orders > 1
}

const expected = {
  client: capture.client,
  recorded_at: capture.recorded_at,
  extension_order_varies: Object.fromEntries(Object.keys(fingerprints).map((t) => [t, Boolean(varies[t])])),
  fingerprints,
}
fs.writeFileSync(output, `${JSON.stringify(expected, null, 2)}\n`)
for (const [transport, hosts] of Object.entries(fingerprints)) {
  console.log(transport, Object.keys(hosts).join(', '), 'varies', expected.extension_order_varies[transport])
}
