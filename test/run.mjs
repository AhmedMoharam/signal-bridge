// Offline tests for signal-bridge: a mock signal-cli daemon, the "phone", and opencode windows (window.mjs) that run
// the real plugin in separate processes against a fake opencode client.
//
//   node test/run.mjs                         all scenarios against ./signal-bridge.ts
//   node test/run.mjs --only order,questions  some of them
//   node test/run.mjs --plugin old.ts         another copy of the plugin (e.g. to watch a fix's test fail first)
//
// Needs Node >= 22.6 (type stripping). Each scenario gets a fresh state directory under $TMPDIR.

import { fork } from "node:child_process"
import fs from "node:fs"
import http from "node:http"
import os from "node:os"
import path from "node:path"
import { fileURLToPath } from "node:url"

const here = path.dirname(fileURLToPath(import.meta.url))
const args = process.argv.slice(2)
const flag = (name) => {
  const i = args.indexOf(name)
  return i >= 0 ? args[i + 1] : undefined
}
const plugin = path.resolve(flag("--plugin") ?? path.join(here, "..", "signal-bridge.ts"))
const only = flag("--only")?.split(",")
const PORT = Number(flag("--port") ?? 18399)
const ACCOUNT = "+10000000000"
const GID = "dGVzdC1ncm91cA=="
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), "signal-bridge-test-"))
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

// ---- mock signal-cli daemon ----

const sent = [] // { kind: "message" | "reaction", ts, text, emoji, target }
let lastTs = 0
const stamp = () => (lastTs = Math.max(Date.now(), lastTs + 1))
const streams = new Set()

const server = http.createServer((req, res) => {
  if (req.url === "/api/v1/check") return res.end("ok")
  if (req.url === "/api/v1/events") {
    res.writeHead(200, { "Content-Type": "text/event-stream" })
    res.write(":\n\n")
    streams.add(res)
    req.on("close", () => streams.delete(res))
    return
  }
  if (req.url === "/api/v1/rpc") {
    let body = ""
    req.on("data", (d) => (body += d))
    req.on("end", () => {
      const { id, method, params } = JSON.parse(body)
      let result
      if (method === "send") {
        const ts = stamp()
        sent.push({ kind: "message", ts, text: params.message })
        result = { timestamp: ts }
      } else if (method === "sendReaction") {
        sent.push({ kind: "reaction", ts: stamp(), emoji: params.emoji, target: params.targetTimestamp })
        result = { timestamp: lastTs }
      } else if (method === "listGroups") result = [{ id: GID, name: "opencode", isMember: true }]
      res.setHeader("Content-Type", "application/json")
      res.end(JSON.stringify({ jsonrpc: "2.0", id, result }))
    })
    return
  }
  res.statusCode = 404
  res.end()
})

/** You, typing in the Signal group on your phone. `quote`: the timestamp of the message you swipe-reply to. */
function phone(text, { quote, clockAhead = 0 } = {}) {
  const ts = stamp()
  const envelope = {
    source: ACCOUNT,
    sourceNumber: ACCOUNT,
    timestamp: ts,
    serverReceivedTimestamp: ts + clockAhead,
    serverDeliveredTimestamp: Date.now() + clockAhead,
    syncMessage: {
      sentMessage: {
        timestamp: ts,
        message: text,
        groupInfo: { groupId: GID, groupName: "opencode", type: "DELIVER" },
        ...(quote ? { quote: { id: quote, author: ACCOUNT, authorNumber: ACCOUNT, text: "…" } } : {}),
      },
    },
  }
  for (const res of streams) res.write(`data: ${JSON.stringify({ account: ACCOUNT, envelope })}\n\n`)
  return ts
}

const messages = () => sent.filter((s) => s.kind === "message")
const mark = () => lastTs
const since = (m) => messages().filter((s) => s.ts > m)

async function until(fn, ms = 5000, what = "a condition") {
  const end = Date.now() + ms
  for (;;) {
    const v = await fn()
    if (v) return v
    if (Date.now() > end) throw new Error(`timed out waiting for ${what}`)
    await sleep(40)
  }
}
const nextMessage = (m, re, ms = 5000) => until(() => since(m).find((s) => re.test(s.text)), ms, `a message matching ${re}`)

// ---- windows ----

let stateDir
const windows = []

async function openWindow(name, { argv = [], options = {} } = {}) {
  const dir = path.join(scratch, "projects", name)
  fs.mkdirSync(dir, { recursive: true })
  const child = fork(path.join(here, "window.mjs"), argv, {
    env: {
      ...process.env,
      XDG_STATE_HOME: stateDir,
      BRIDGE_PLUGIN: plugin,
      BRIDGE_DIR: dir,
      BRIDGE_OPTIONS: JSON.stringify({ enabled: true, account: ACCOUNT, groupId: GID, port: PORT, startDaemon: false, ...options }),
    },
    execArgv: ["--experimental-strip-types", "--no-warnings"],
    stdio: ["ignore", "inherit", "inherit", "ipc"],
  })
  const pending = new Map()
  let n = 0
  child.on("message", (m) => {
    const p = pending.get(m.id)
    pending.delete(m.id)
    if (m.error) p?.reject(new Error(m.error))
    else p?.resolve(m.result)
  })
  const call = (cmd, a) =>
    new Promise((resolve, reject) => {
      const id = ++n
      pending.set(id, { resolve, reject })
      child.send({ id, cmd, args: a })
    })
  await call("start")
  const w = { name, child, call, pid: child.pid, calls: async () => (await call("state")).calls }
  windows.push(w)
  return w
}

async function closeAll() {
  for (const w of windows.splice(0)) {
    try {
      process.kill(w.pid, "SIGCONT")
    } catch {}
    if (w.child.connected) await Promise.race([w.call("dispose"), sleep(1000)]).catch(() => {})
    w.child.kill("SIGKILL")
  }
}

// ---- checks ----

let failures = 0
let passes = 0
function check(ok, label, detail) {
  if (ok) passes++
  else failures++
  console.log(`  ${ok ? "ok  " : "FAIL"} ${label}${!ok && detail !== undefined ? `\n         got: ${typeof detail === "string" ? detail : JSON.stringify(detail)}` : ""}`)
}

const q = (header, labels, extra = {}) => ({
  header,
  question: `${header}?`,
  options: labels.map((label) => ({ label, description: `about ${label}` })),
  ...extra,
})
const replyPosts = (calls) => calls.filter((c) => c.type === "post" && c.url === "/question/{requestID}/reply")
const prompts = (calls) => calls.filter((c) => c.type === "prompt")

// ---- scenarios ----

const scenarios = {
  async basics() {
    const t0 = mark()
    const w = await openWindow("alpha")
    await nextMessage(t0, /🟢 opencode connected — alpha/)
    check(true, "announces itself")
    let m = mark()
    phone("/help")
    check(!!(await nextMessage(m, /\/projects/)), "/help lists the commands, /projects among them")
    m = mark()
    phone("first prompt")
    await until(async () => prompts(await w.calls()).length === 1, 3000, "the prompt")
    const [p] = prompts(await w.calls())
    check(p.text === "first prompt", "a plain message becomes a prompt", p)
    w.call("reply", { sessionID: p.sessionID, text: "the answer" })
    w.call("status", { sessionID: p.sessionID, type: "idle" })
    const done = await nextMessage(m, /✅ alpha/)
    check(done.text.endsWith("the answer"), "the finished reply is posted", done.text)
    check(sent.some((s) => s.kind === "reaction" && s.emoji === "👀"), "the prompt gets 👀")
  },

  // Issue 1, as measured on 2026-09-27: `/new` at 13:59:30, and the next prompt still landed in the old session.
  async newSticks() {
    const w = await openWindow("alpha")
    await sleep(3200)
    const old = await w.call("session", { title: "Old work" })
    await w.call("type", { sessionID: old.id, text: "keep working on the old thing" })
    let m = mark()
    phone("/new")
    const made = await nextMessage(m, /🆕/)
    check(!!made, "/new answers")
    // The old session keeps running and reports busy again.
    await w.call("status", { sessionID: old.id, type: "busy" })
    m = mark()
    phone("start the new thing")
    await until(async () => prompts(await w.calls()).length, 3000, "the prompt")
    const [p] = prompts(await w.calls())
    check(p.sessionID !== old.id, "the prompt after /new goes to the new session, not the old one", p)
    const selects = (await w.calls()).filter((c) => c.url === "/tui/select-session")
    check(selects.length === 1 && selects[0].body.sessionID === p.sessionID, "the terminal is switched to the new session", selects)
  },

  // Issue 1, with a question waiting: it must not swallow the first prompt of the new session.
  async newReleasesQuestion() {
    const w = await openWindow("alpha")
    await sleep(3200)
    const old = await w.call("session", { title: "Old work" })
    await w.call("type", { sessionID: old.id, text: "old" })
    let m = mark()
    const qid = await w.call("question", { sessionID: old.id, questions: [q("Pick", ["A", "B"])] })
    const ask = await nextMessage(m, /❓ alpha/)
    m = mark()
    phone("/new")
    await nextMessage(m, /🆕/)
    phone("start the new thing")
    await until(async () => (await w.calls()).some((c) => c.type === "prompt" || c.url === "/question/{requestID}/reply"), 3000, "the prompt")
    let calls = await w.calls()
    check(prompts(calls).some((c) => c.text === "start the new thing" && c.sessionID !== old.id), "the prompt reaches the new session", calls)
    check(replyPosts(calls).length === 0, "the prompt is not taken as the answer to the old question", replyPosts(calls))
    // The old question can still be answered by replying to it.
    phone("2", { quote: ask.ts })
    await until(async () => replyPosts(await w.calls()).length, 3000, "the answer")
    const [r] = replyPosts(await w.calls())
    check(r?.path.requestID === qid && JSON.stringify(r.body.answers) === '[["B"]]', "a swipe-reply answers the old question", r)
  },

  // Issue 2: five questions in one request — measured 2026-10-02 and 2026-10-04 in Suhba.
  async questions() {
    const w = await openWindow("alpha")
    await sleep(3200)
    const s = await w.call("session", { title: "Planner" })
    await w.call("type", { sessionID: s.id, text: "plan" })
    let m = mark()
    const five = ["Times", "Types", "Days", "Save", "Lang"].map((h) => q(h, ["one", "two", "three"]))
    const qid = await w.call("question", { sessionID: s.id, questions: five })
    const ask = await nextMessage(m, /❓ alpha/)
    await sleep(300)
    const shown = since(m).map((x) => x.text).join("\n")
    check(["Q1 · Times", "Q2 · Types", "Q3 · Days", "Q4 · Save", "Q5 · Lang"].every((h) => shown.includes(h)), "all five questions are shown at once", shown)
    m = mark()
    phone("1\n2\n3\n1\n2")
    await until(async () => replyPosts(await w.calls()).length, 3000, "the answers")
    const [r] = replyPosts(await w.calls())
    check(r?.path.requestID === qid && JSON.stringify(r.body.answers) === JSON.stringify([["one"], ["two"], ["three"], ["one"], ["two"]]), "one line per question answers all five", r?.body)

    // Some now, the rest later, out of order, with a typed answer.
    m = mark()
    const q3 = await w.call("question", { sessionID: s.id, questions: [q("A", ["x", "y"]), q("B", ["x", "y"]), q("C", ["x", "y"])] })
    await nextMessage(m, /❓ alpha/)
    m = mark()
    phone("2")
    const more = await nextMessage(m, /more to go/)
    check(!more.text.includes("Q1 ·") && more.text.includes("Q2 ·") && more.text.includes("Q3 ·"), "after one answer only the open questions are shown again", more.text)
    phone("Q3: my own words\nQ2: 1")
    await until(async () => replyPosts(await w.calls()).length === 2, 3000, "the second answers")
    const r2 = replyPosts(await w.calls())[1]
    check(r2.path.requestID === q3 && JSON.stringify(r2.body.answers) === JSON.stringify([["y"], ["x"], ["my own words"]]), "tagged lines answer specific questions", r2.body)

    // Not understood.
    m = mark()
    await w.call("question", { sessionID: s.id, questions: [q("Strict", ["x", "y"], { custom: false })] })
    await nextMessage(m, /❓ alpha/)
    m = mark()
    phone("whatever")
    const bad = await nextMessage(m, /Didn't catch/)
    check(/pick a number from 1 to 2/.test(bad.text), "an answer that does not fit says why", bad.text)
    check(replyPosts(await w.calls()).length === 2, "and answers nothing")
    phone("/skip")
    await until(async () => (await w.calls()).some((c) => c.url === "/question/{requestID}/reject"), 3000, "the skip")
    check(true, "/skip dismisses it")
  },

  // Long texts are split into messages Signal shows inline, instead of one "Read more" attachment.
  async longText() {
    const w = await openWindow("alpha")
    await sleep(3200)
    const s = await w.call("session", { title: "Long" })
    await w.call("type", { sessionID: s.id, text: "go" })
    const long = Array.from({ length: 6 }, (_, i) =>
      q(`Header ${i + 1}`, ["first", "second", "third", "fourth"].map((l) => `${l} — ${"وصف طويل ".repeat(6)}`)),
    )
    let m = mark()
    await w.call("question", { sessionID: s.id, questions: long })
    await nextMessage(m, /❓ alpha/)
    await sleep(500)
    const parts = since(m)
    const sizes = parts.map((p) => Buffer.byteLength(p.text))
    check(parts.length > 1 && sizes.every((n) => n <= 2000), "a long question list arrives in several messages under 2000 bytes", sizes)
    check(parts.every((p, i) => p.text.endsWith(`(${i + 1}/${parts.length})`)), "numbered in order", parts.map((p) => p.text.slice(-8)))
    const joined = parts.map((p) => p.text.replace(/\n\(\d+\/\d+\)$/, "")).join("\n")
    check(long.every((x) => joined.includes(x.header)), "nothing is lost")
  },

  // Issue 4: messages leave in the order of the events that caused them.
  async order() {
    const w = await openWindow("alpha")
    await sleep(3200)
    const a = await w.call("session", { title: "Slow history" })
    const b = await w.call("session", { title: "Other" })
    await w.call("type", { sessionID: a.id, text: "task" })
    await w.call("reply", { sessionID: a.id, text: "task done" })
    await w.call("delay", { messages: 1500 })
    let m = mark()
    w.call("status", { sessionID: a.id, type: "idle" })
    const asked = w.call("question", { sessionID: b.id, questions: [q("Next", ["x", "y"])] })
    await nextMessage(m, /❓ alpha/, 6000)
    await nextMessage(m, /✅ alpha/, 6000)
    const order = since(m).map((x) => x.text.slice(0, 12))
    check(/✅/.test(order[0]) && /❓/.test(order[1]), "the finished reply comes before the question that followed it", order)

    // A prompt typed right after the run ended is not part of the run that ended.
    await w.call("delay", { messages: 0 })
    await w.call("answeredInTerminal", { requestID: await asked })
    const c = await w.call("session", { title: "Next prompt" })
    await w.call("type", { sessionID: c.id, text: "one" })
    await w.call("reply", { sessionID: c.id, text: "answer one" })
    await w.call("delay", { messages: 800 })
    m = mark()
    w.call("status", { sessionID: c.id, type: "idle" })
    w.call("type", { sessionID: c.id, text: "two" })
    const fin = await nextMessage(m, /✅ alpha · Next prompt/, 4000)
    check(fin.text.endsWith("answer one"), "the reply posted is the finished run's, not the next prompt's", fin.text)
    await w.call("delay", { messages: 0 })
    await w.call("reply", { sessionID: c.id, text: "answer two" })
    await w.call("status", { sessionID: c.id, type: "idle" })

    // Prompts queued while it was working each get their answer.
    const d = await w.call("session", { title: "Queued" })
    const p1 = await w.call("type", { sessionID: d.id, text: "p1" })
    const p2 = await w.call("user", { sessionID: d.id, text: "p2" })
    await w.call("reply", { sessionID: d.id, text: "reply to p1", parentID: p1 })
    await w.call("reply", { sessionID: d.id, text: "reply to p2", parentID: p2 })
    m = mark()
    await w.call("status", { sessionID: d.id, type: "idle" })
    const both = await nextMessage(m, /✅ alpha · Queued/)
    check(both.text.includes("reply to p1") && both.text.includes("reply to p2"), "both queued prompts' replies are posted", both.text)

    // Compaction in the middle of a run: its summary is not the reply.
    const e = await w.call("session", { title: "Compacted" })
    await w.call("type", { sessionID: e.id, text: "big job" })
    await w.call("reply", { sessionID: e.id, text: "working on it" })
    await w.call("user", { sessionID: e.id, compaction: true })
    await w.call("reply", { sessionID: e.id, text: "SUMMARY of everything", summary: true })
    await w.call("user", { sessionID: e.id, text: "Continue if you have next steps", synthetic: true })
    await w.call("reply", { sessionID: e.id, text: "final answer" })
    m = mark()
    await w.call("status", { sessionID: e.id, type: "idle" })
    const comp = await nextMessage(m, /✅ alpha · Compacted/)
    check(comp.text.endsWith("final answer") && !comp.text.includes("SUMMARY"), "after compaction the final answer is posted, not the summary", comp.text)
  },

  // Issue 3: two projects at once — measured 2026-10-02, Suhba's window never received a message.
  async projects() {
    let m = mark()
    const alpha = await openWindow("alpha")
    await nextMessage(m, /connected — alpha/)
    m = mark()
    const beta = await openWindow("beta")
    const hello = await nextMessage(m, /connected — beta/)
    check(/2 windows open/.test(hello.text), "the second window says two are open", hello.text)
    const sa = await alpha.call("session", { title: "Alpha work" })
    const sb = await beta.call("session", { title: "Beta work" })

    m = mark()
    phone("/projects")
    const list = await nextMessage(m, /Open opencode windows/)
    check(/1\. alpha/.test(list.text) && /2\. beta/.test(list.text), "/projects lists both", list.text)

    m = mark()
    phone("/project 2")
    await nextMessage(m, /Now talking to beta/)
    phone("hello beta")
    await until(async () => prompts(await beta.calls()).length, 4000, "beta's prompt")
    check(prompts(await alpha.calls()).length === 0, "a prompt after /project 2 reaches beta, not alpha")

    m = mark()
    phone("/status")
    const st = await nextMessage(m, /📍/)
    check(/beta/.test(st.text) && /Also open: alpha/.test(st.text), "/status is answered by beta and mentions alpha", st.text)

    // alpha asks while you talk to beta: your next plain message answers it.
    m = mark()
    const qa = await alpha.call("question", { sessionID: sa.id, questions: [q("Alpha asks", ["yes", "no"])] })
    const ask = await nextMessage(m, /❓ alpha/)
    await sleep(300)
    phone("1")
    await until(async () => replyPosts(await alpha.calls()).length, 4000, "alpha's answer")
    check(replyPosts(await alpha.calls())[0].path.requestID === qa, "a plain answer reaches the window that asked")
    check(prompts(await beta.calls()).length === 1, "and is not sent to beta as a prompt")

    // A swipe-reply to alpha's finished message prompts alpha's session, though you are talking to beta.
    await alpha.call("type", { sessionID: sa.id, text: "work" })
    await alpha.call("reply", { sessionID: sa.id, text: "alpha finished" })
    m = mark()
    await alpha.call("status", { sessionID: sa.id, type: "idle" })
    const fin = await nextMessage(m, /✅ alpha/)
    m = mark()
    phone("/project 2")
    await nextMessage(m, /Now talking to beta/)
    const before = prompts(await alpha.calls()).length
    phone("follow up on that", { quote: fin.ts })
    await until(async () => prompts(await alpha.calls()).length > before, 4000, "the follow-up")
    const last = prompts(await alpha.calls()).at(-1)
    check(last.sessionID === sa.id && last.text === "follow up on that", "a swipe-reply prompts the session it replies to", last)

    // Typing in beta's terminal makes beta the one you talk to.
    m = mark()
    phone("/project 1")
    await nextMessage(m, /Now talking to alpha/)
    await beta.call("type", { sessionID: sb.id, text: "typed in beta's terminal" })
    await sleep(300)
    const nBeta = prompts(await beta.calls()).length
    phone("from the phone")
    await until(async () => prompts(await beta.calls()).length > nBeta, 4000, "beta's prompt")
    check(prompts(await beta.calls()).at(-1).sessionID === sb.id, "after typing in beta's terminal, Signal talks to beta's session")

    // The routing window (alpha) is killed: beta takes over.
    alpha.child.kill("SIGKILL")
    windows.splice(windows.indexOf(alpha), 1)
    await sleep(11_000)
    m = mark()
    phone("/projects")
    const after = await nextMessage(m, /Open opencode windows/, 8000)
    check(!/alpha/.test(after.text) && /beta/.test(after.text), "after alpha dies beta reads Signal and alpha is gone from /projects", after.text)
  },

  // A window that stops reading its inbox: you are told, instead of silence.
  async stuckWindow() {
    let m = mark()
    const alpha = await openWindow("alpha")
    const beta = await openWindow("beta")
    await nextMessage(m, /connected — beta/)
    phone("/project 2")
    await nextMessage(m, /Now talking to beta/)
    process.kill(beta.pid, "SIGSTOP")
    m = mark()
    phone("are you there")
    const warn = await nextMessage(m, /beta did not pick up your message/, 30_000)
    check(!!warn, "a message for a window that does not take it is reported")
    process.kill(beta.pid, "SIGCONT")
  },

  // Signal sorts by the sender's clock: a computer clock behind Signal's is reported.
  async clock() {
    await openWindow("alpha")
    await sleep(3200)
    const m = mark()
    phone("/status", { clockAhead: 12_000 })
    const warn = await nextMessage(m, /clock is 1[12]s behind/)
    check(!!warn, "a computer clock 12s behind Signal's is reported")
  },

  // `opencode run` posts its results but never reads Signal.
  async headless() {
    let m = mark()
    const run = await openWindow("job", { argv: ["run"] })
    await sleep(3500)
    check(!since(m).some((s) => /connected — job/.test(s.text)), "opencode run does not announce itself")
    const lock = path.join(stateDir, "opencode-signal-bridge", "inbound.lock")
    check(!fs.existsSync(lock), "and does not take the inbound lock")
    const s = await run.call("session", { title: "Job" })
    await run.call("type", { sessionID: s.id, text: "do it" })
    await run.call("reply", { sessionID: s.id, text: "job done" })
    m = mark()
    await run.call("status", { sessionID: s.id, type: "idle" })
    check(!!(await nextMessage(m, /✅ job · Job/)), "but posts its finished reply")
  },
}

// ---- main ----

await new Promise((resolve) => server.listen(PORT, "127.0.0.1", resolve))
console.log(`plugin: ${plugin}`)
for (const [name, run] of Object.entries(scenarios)) {
  if (only && !only.includes(name)) continue
  console.log(`\n${name}`)
  stateDir = fs.mkdtempSync(path.join(scratch, `${name}-`))
  try {
    await run()
  } catch (e) {
    check(false, `${name} did not complete`, String(e?.message ?? e))
  }
  await closeAll()
}
server.close()
for (const res of streams) res.end()
console.log(`\n${passes} passed, ${failures} failed`)
fs.rmSync(scratch, { recursive: true, force: true })
process.exit(failures ? 1 : 0)
