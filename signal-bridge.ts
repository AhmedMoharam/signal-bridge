/**
 * signal-bridge — use opencode from Signal.
 *
 * Enable it with one entry in ~/.config/opencode/opencode.jsonc (see README.md):
 *
 *   "plugin": [["./signal-bridge/signal-bridge.ts", { "enabled": true, "account": "+15551234567" }]]
 *
 * What it does while opencode is running:
 *   - when a session finishes, its final reply is posted to a Signal group
 *   - questions (the `question` tool) and permission requests are posted there, and your reply answers them
 *   - anything else you type in the group is sent to opencode as a prompt
 *
 * signal-cli runs as a linked device on your own Signal account. Messages you type in the group reach it as
 * sync messages; messages it sends come from "you", so they do not make your phone ring — `ntfyTopic` adds a
 * push notification for that.
 *
 * Verified against opencode 1.18.29 and signal-cli 0.14.8 source.
 */
import type { Plugin } from "@opencode-ai/plugin"
import { spawn, type ChildProcess } from "node:child_process"
import fs from "node:fs"
import os from "node:os"
import path from "node:path"

type Options = {
  enabled?: boolean
  /** Your Signal number in E.164 form, e.g. "+15551234567". */
  account?: string
  /** Name of the Signal group used as the chat (a group with only you in it). */
  groupName?: string
  /** Group id (base64). Optional — resolved from groupName when omitted. */
  groupId?: string
  signalCli?: string
  host?: string
  port?: number
  /** Start `signal-cli daemon` when it is not already running. */
  startDaemon?: boolean
  ntfyTopic?: string
  ntfyServer?: string
  /** "always": post every finished reply. "signal": only replies to prompts sent from Signal. */
  notify?: "always" | "signal"
  forwardQuestions?: boolean
  forwardPermissions?: boolean
  maxReplyChars?: number
}

const STATE_DIR = path.join(
  process.env.XDG_STATE_HOME || path.join(os.homedir(), ".local", "state"),
  "opencode-signal-bridge",
)
const LOCK_FILE = path.join(STATE_DIR, "inbound.lock")
const LOCK_STALE_MS = 60_000

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms))

function str(value: unknown): string {
  if (typeof value === "string") return value
  if (value instanceof Error) return value.message
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

function log(tag: string, ...parts: unknown[]) {
  try {
    fs.mkdirSync(STATE_DIR, { recursive: true })
    fs.appendFileSync(
      path.join(STATE_DIR, "bridge.log"),
      `${new Date().toISOString()} [${tag}] ${parts.map(str).join(" ")}\n`,
    )
  } catch {}
}

// ---------------------------------------------------------------------------------------------------------------
// signal-cli daemon — one per process, shared by every plugin instance (one instance per project directory)
// ---------------------------------------------------------------------------------------------------------------

let daemon: ChildProcess | undefined
let daemonStarting: Promise<boolean> | undefined
let daemonRetryAt = 0
let liveInstances = 0

async function daemonUp(base: string) {
  try {
    const res = await fetch(`${base}/api/v1/check`, { signal: AbortSignal.timeout(2000) })
    return res.ok
  } catch {
    return false
  }
}

function ensureDaemon(base: string, bin: string, account: string, listen: string, allowStart: boolean) {
  if (daemonStarting) return daemonStarting
  daemonStarting = (async () => {
    if (await daemonUp(base)) return true
    if (!allowStart || Date.now() < daemonRetryAt) return false

    if (!daemon || daemon.exitCode !== null) {
      fs.mkdirSync(STATE_DIR, { recursive: true })
      const out = fs.openSync(path.join(STATE_DIR, "signal-cli.log"), "a")
      const args = ["-a", account, "daemon", "--http", listen, "--no-receive-stdout"]
      args.push("--ignore-attachments", "--ignore-stories", "--ignore-avatars", "--ignore-stickers")
      log("daemon", `starting ${bin} ${args.join(" ")}`)
      // opencode can exit without disposing plugins (measured: SIGTERM to `opencode serve`), which would leave
      // signal-cli running. The shell watchdog stops it within 5s of this process disappearing.
      const watchdog = [
        `"$0" "$@" & d=$!`,
        `trap 'kill $d 2>/dev/null' EXIT`,
        `trap 'kill $d 2>/dev/null; exit' INT TERM`,
        `while kill -0 ${process.pid} 2>/dev/null && kill -0 $d 2>/dev/null; do sleep 5; done`,
      ].join("\n")
      try {
        daemon = spawn("/bin/sh", ["-c", watchdog, bin, ...args], { stdio: ["ignore", out, out] })
        daemon.on("error", (e) => log("daemon", "spawn error:", e))
        daemon.on("exit", (code, signal) => log("daemon", `exited code=${code} signal=${signal}`))
      } catch (e) {
        log("daemon", "spawn failed:", e)
        daemonRetryAt = Date.now() + 60_000
        return false
      } finally {
        fs.closeSync(out)
      }
    }

    for (let i = 0; i < 90; i++) {
      await sleep(1000)
      if (await daemonUp(base)) return true
      if (!daemon || daemon.exitCode !== null) break
    }
    log("daemon", `not reachable at ${base}; see ${path.join(STATE_DIR, "signal-cli.log")}`)
    daemonRetryAt = Date.now() + 60_000
    return false
  })().finally(() => {
    daemonStarting = undefined
  })
  return daemonStarting
}

function stopDaemon() {
  if (daemon && daemon.exitCode === null) {
    log("daemon", "stopping")
    daemon.kill("SIGTERM")
  }
  daemon = undefined
}

process.once("exit", stopDaemon)

// ---------------------------------------------------------------------------------------------------------------
// Inbound lock — every event-stream subscriber receives every message, so only one opencode acts on them
// ---------------------------------------------------------------------------------------------------------------

type Lock = { pid: number; token: string; directory: string; time: number }

function readLock(): Lock | undefined {
  try {
    return JSON.parse(fs.readFileSync(LOCK_FILE, "utf8"))
  } catch {
    return undefined
  }
}

function lockStale(lock: Lock) {
  if (Date.now() - lock.time > LOCK_STALE_MS) return true
  try {
    process.kill(lock.pid, 0)
    return false
  } catch (e: any) {
    return e?.code === "ESRCH"
  }
}

function holdLock(token: string, directory: string) {
  const body = JSON.stringify({ pid: process.pid, token, directory, time: Date.now() } satisfies Lock)
  try {
    fs.mkdirSync(STATE_DIR, { recursive: true })
    const current = readLock()
    if (current?.token === token) {
      fs.writeFileSync(LOCK_FILE, body)
      return true
    }
    if (current && !lockStale(current)) return false
    if (current) fs.rmSync(LOCK_FILE, { force: true })
    fs.writeFileSync(LOCK_FILE, body, { flag: "wx" })
    return true
  } catch {
    return false
  }
}

function releaseLock(token: string) {
  try {
    if (readLock()?.token === token) fs.rmSync(LOCK_FILE, { force: true })
  } catch {}
}

// ---------------------------------------------------------------------------------------------------------------
// Waiting questions and permission requests
// ---------------------------------------------------------------------------------------------------------------

type QuestionInfo = {
  question: string
  header: string
  options: { label: string; description: string }[]
  multiple?: boolean
  custom?: boolean
}

type Waiting =
  | {
      kind: "question"
      id: string
      sessionID: string
      questions: QuestionInfo[]
      index: number
      answers: string[][]
      shown: boolean
    }
  | {
      kind: "permission"
      id: string
      sessionID: string
      permission: string
      patterns: string[]
      metadata: Record<string, unknown>
      shown: boolean
    }

function answerHint(q: QuestionInfo) {
  let hint = q.multiple ? "Reply with numbers, e.g. 1,3" : "Reply with a number"
  if (q.custom !== false) hint += " or type your own answer"
  return `${hint}. /skip to dismiss.`
}

function parseAnswer(q: QuestionInfo, text: string): string[] | undefined {
  const t = text.trim()
  if (/^\d+([\s,]+\d+)*$/.test(t)) {
    const picks = [...new Set(t.split(/[\s,]+/).map(Number))]
    if (picks.some((n) => n < 1 || n > q.options.length)) return undefined
    const labels = picks.map((n) => q.options[n - 1].label)
    return q.multiple ? labels : labels.slice(0, 1)
  }
  const exact = q.options.find((o) => o.label.toLowerCase() === t.toLowerCase())
  if (exact) return [exact.label]
  if (q.custom === false || !t) return undefined
  return [t]
}

// ---------------------------------------------------------------------------------------------------------------
// The plugin
// ---------------------------------------------------------------------------------------------------------------

const SignalBridge: Plugin = async ({ client, directory, worktree }, options) => {
  const opts = (options ?? {}) as Options
  const project = path.basename(worktree && worktree !== "/" ? worktree : directory)
  const L = (...parts: unknown[]) => log(project, ...parts)
  if (!opts.enabled) return {}
  if (!opts.account) {
    L("not started: the `account` option is missing")
    return {}
  }

  const account = opts.account
  const groupName = opts.groupName ?? "opencode"
  const listen = `${opts.host ?? "127.0.0.1"}:${opts.port ?? 18351}`
  const base = `http://${listen}`
  const localBin = path.join(os.homedir(), ".local", "bin", "signal-cli")
  const bin = opts.signalCli ?? (fs.existsSync(localBin) ? localBin : "signal-cli")
  const notify = opts.notify ?? "always"
  const maxReply = opts.maxReplyChars ?? 6000
  const token = `${process.pid}-${Math.random().toString(36).slice(2)}`

  let groupId = opts.groupId
  let stopped = false
  let owner = false
  let events: AbortController | undefined

  const sessions = new Map<string, { title: string; parentID?: string }>()
  const busy = new Set<string>()
  const fromSignal = new Set<string>()
  let activeSession: string | undefined
  let listing: string[] = []

  const waiting: Waiting[] = []
  const answeredHere = new Set<string>()
  const sentByUs = new Set<number>()

  liveInstances++
  L(`enabled (signal-cli at ${base}, group "${groupName}")`)

  // ---- opencode API ----

  // Question and permission replies are not in the v1 SDK the plugin receives, so go through its HTTP client,
  // which already knows how to reach this instance (in-process or over the local server).
  async function api(method: "get" | "post", url: string, init: { path?: object; body?: object } = {}) {
    const http = (client as any)._client
    if (!http || typeof http[method] !== "function") throw new Error("opencode client has no raw request method")
    const res = await http[method]({
      url,
      ...init,
      ...(init.body ? { headers: { "Content-Type": "application/json" } } : {}),
    })
    if (res?.error) throw new Error(`${method.toUpperCase()} ${url}: ${str(res.error)}`)
    return res?.data
  }

  async function sessionInfo(id: string) {
    const cached = sessions.get(id)
    if (cached) return cached
    const res = await client.session.get({ path: { id } })
    if (!res.data) return undefined
    const info = { title: res.data.title, parentID: res.data.parentID }
    sessions.set(id, info)
    return info
  }

  async function title(id: string) {
    return (await sessionInfo(id).catch(() => undefined))?.title || id.slice(0, 16)
  }

  async function rootSessions() {
    const res = await client.session.list()
    return (res.data ?? []).filter((s) => !s.parentID).sort((a, b) => b.time.updated - a.time.updated)
  }

  // ---- Signal ----

  let rpcSeq = 0
  async function rpc(method: string, params: Record<string, unknown>) {
    const request = () =>
      fetch(`${base}/api/v1/rpc`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: ++rpcSeq, method, params }),
        signal: AbortSignal.timeout(60_000),
      })
    // One retry for connection-level failures (e.g. a reused keep-alive socket the server already closed).
    const res = await request().catch(async (e) => {
      if (e?.name === "TimeoutError") throw e
      L(`signal-cli ${method}: ${str(e)} — retrying once`)
      await sleep(500)
      return request()
    })
    const text = await res.text()
    let body: any
    try {
      body = JSON.parse(text)
    } catch {
      throw new Error(`signal-cli ${method}: HTTP ${res.status} ${text.slice(0, 200)}`)
    }
    if (body.error) throw new Error(`signal-cli ${method}: ${body.error.message ?? str(body.error)}`)
    return body.result
  }

  const daemonReady = () => ensureDaemon(base, bin, account, listen, opts.startDaemon !== false)

  async function resolveGroup() {
    if (groupId) return groupId
    const groups = await rpc("listGroups", {})
    const hit = (Array.isArray(groups) ? groups : []).find((g: any) => g?.name === groupName && g?.isMember !== false)
    if (hit?.id) {
      groupId = hit.id
      L(`using Signal group "${groupName}"`)
    }
    return groupId
  }

  let outbox = Promise.resolve()
  function send(text: string) {
    outbox = outbox.then(async () => {
      if (stopped) return
      try {
        if (!(await daemonReady())) throw new Error(`signal-cli daemon is not reachable at ${base}`)
        const gid = await resolveGroup()
        if (!gid) throw new Error(`no Signal group named "${groupName}" yet — send a message in it from your phone`)
        const result = await rpc("send", { groupId: gid, message: text })
        if (typeof result?.timestamp === "number") {
          sentByUs.add(result.timestamp)
          if (sentByUs.size > 200) sentByUs.delete(sentByUs.values().next().value!)
        }
      } catch (e) {
        L("send failed:", e)
      }
    })
    return outbox
  }

  async function react(timestamp: number, emoji: string) {
    if (!groupId) return
    await rpc("sendReaction", { groupId, emoji, targetAuthor: account, targetTimestamp: timestamp }).catch((e) =>
      L("reaction failed:", e),
    )
  }

  async function buzz(heading: string, tags: string) {
    if (!opts.ntfyTopic) return
    const server = (opts.ntfyServer ?? "https://ntfy.sh").replace(/\/+$/, "")
    try {
      await fetch(`${server}/${opts.ntfyTopic}`, {
        method: "POST",
        // Header values must be ASCII; the body can be anything. Keep content out of a public push service.
        headers: { Title: heading, Priority: "4", Tags: tags },
        body: `${project} — open Signal`,
        signal: AbortSignal.timeout(10_000),
      })
    } catch (e) {
      L("ntfy failed:", e)
    }
  }

  // ---- waiting questions / permissions ----

  async function questionText(item: Extract<Waiting, { kind: "question" }>) {
    const q = item.questions[item.index]
    const count = item.questions.length > 1 ? ` (${item.index + 1}/${item.questions.length})` : ""
    const lines = [`❓ ${project} · ${await title(item.sessionID)}`, `${q.header}${count}`, q.question, ""]
    q.options.forEach((o, i) => lines.push(`${i + 1}. ${o.label}${o.description ? ` — ${o.description}` : ""}`))
    lines.push("", answerHint(q))
    return lines.join("\n")
  }

  async function permissionText(item: Extract<Waiting, { kind: "permission" }>) {
    const detail = [item.metadata?.command, item.metadata?.filepath, item.metadata?.url]
      .filter((v) => typeof v === "string")
      .slice(0, 1)
    const patterns = item.patterns.slice(0, 5).map((p) => `  ${p}`)
    return [
      `🔐 ${project} · ${await title(item.sessionID)}`,
      `opencode wants permission: ${item.permission}`,
      ...detail.map((d) => `  ${String(d).slice(0, 500)}`),
      ...(detail.length ? [] : patterns),
      "",
      "Reply y = allow once · a = always allow · n = deny",
      "(or type a reason to deny with feedback)",
    ].join("\n")
  }

  async function showHead() {
    const item = waiting[0]
    if (!item || item.shown) return
    item.shown = true
    const more = waiting.length > 1 ? `\n\n(+${waiting.length - 1} more waiting)` : ""
    const text = item.kind === "question" ? await questionText(item) : await permissionText(item)
    await send(text + more)
  }

  function dropWaiting(id: string) {
    const index = waiting.findIndex((w) => w.id === id)
    if (index >= 0) waiting.splice(index, 1)
    void showHead()
  }

  async function answerHead(text: string, timestamp: number) {
    const item = waiting[0]
    const t = text.trim()

    if (item.kind === "permission") {
      const reply = /^(y|yes|ok|once|allow)$/i.test(t) ? "once" : /^(a|always)$/i.test(t) ? "always" : "reject"
      const message = reply === "reject" && !/^(n|no|deny|reject)$/i.test(t) ? t : undefined
      answeredHere.add(item.id)
      // Rejecting one request makes opencode reject the session's other pending permissions too.
      if (reply === "reject")
        waiting.filter((w) => w.kind === "permission" && w.sessionID === item.sessionID).forEach((w) => answeredHere.add(w.id))
      await api("post", "/permission/{requestID}/reply", {
        path: { requestID: item.id },
        body: { reply, ...(message ? { message } : {}) },
      })
      await react(timestamp, reply === "reject" ? "🚫" : "👍")
      dropWaiting(item.id)
      return
    }

    const q = item.questions[item.index]
    const answer = parseAnswer(q, t)
    if (!answer) {
      await send(`Didn't catch that. ${answerHint(q)}`)
      return
    }
    item.answers.push(answer)
    item.index++
    await react(timestamp, "👍")
    if (item.index < item.questions.length) {
      await send(await questionText(item))
      return
    }
    answeredHere.add(item.id)
    await api("post", "/question/{requestID}/reply", { path: { requestID: item.id }, body: { answers: item.answers } })
    dropWaiting(item.id)
  }

  async function skipHead(timestamp: number) {
    const item = waiting[0]
    if (!item) return send("Nothing is waiting for an answer.")
    answeredHere.add(item.id)
    if (item.kind === "question") {
      await api("post", "/question/{requestID}/reject", { path: { requestID: item.id } })
    } else {
      waiting.filter((w) => w.kind === "permission" && w.sessionID === item.sessionID).forEach((w) => answeredHere.add(w.id))
      await api("post", "/permission/{requestID}/reply", { path: { requestID: item.id }, body: { reply: "reject" } })
    }
    await react(timestamp, "👌")
    dropWaiting(item.id)
  }

  function onAnsweredElsewhere(requestID: string) {
    const index = waiting.findIndex((w) => w.id === requestID)
    if (answeredHere.delete(requestID) || index < 0) {
      if (index >= 0) dropWaiting(requestID)
      return
    }
    const [item] = waiting.splice(index, 1)
    if (item.shown) void send(`✔️ Answered in opencode (${item.kind === "question" ? "question" : item.permission}).`)
    void showHead()
  }

  function addWaiting(item: Waiting, heading: string, tags: string) {
    waiting.push(item)
    void showHead()
    void buzz(heading, tags)
  }

  // ---- prompts and commands from Signal ----

  async function sendPrompt(text: string, timestamp: number) {
    let id = activeSession ?? (await rootSessions())[0]?.id
    if (!id) {
      const created = await client.session.create({ body: {} })
      if (!created.data) throw new Error(`could not create a session: ${str(created.error)}`)
      id = created.data.id
    }

    // Reuse the agent and model of the session's last prompt so Signal prompts behave like the terminal ones.
    const history = await client.session.messages({ path: { id } })
    const lastUser = [...(history.data ?? [])].reverse().find((m) => m.info.role === "user")?.info as any
    const body: any = { parts: [{ type: "text", text }] }
    if (lastUser?.agent) body.agent = lastUser.agent
    if (lastUser?.model?.providerID) body.model = { providerID: lastUser.model.providerID, modelID: lastUser.model.modelID }

    // Read this before prompting: the prompt itself marks the session busy before promptAsync returns.
    const wasBusy = busy.has(id)
    const res = await client.session.promptAsync({ path: { id }, body })
    if (res.error) throw new Error(`prompt failed: ${str(res.error)}`)

    activeSession = id
    fromSignal.add(id)
    await react(timestamp, "👀")
    const name = await title(id)
    if (wasBusy) await send(`📨 Queued for "${name}" — it is still working on the previous task.`)
    void client.tui
      .showToast({ body: { title: "Signal", message: `Prompt from Signal → ${name}`, variant: "info" } })
      .catch(() => {})
  }

  async function command(text: string, timestamp: number) {
    const [name, ...rest] = text.slice(1).split(/\s+/)
    const arg = rest.join(" ").trim()
    switch (name.toLowerCase()) {
      case "help":
      case "start":
        return send(
          [
            `opencode · ${project}`,
            "Type anything to send it as a prompt to the current session.",
            "When a question or permission request is waiting, your next message answers it.",
            "",
            "/status — what is running",
            "/sessions — recent sessions",
            "/use N — switch to session N from /sessions",
            "/new [title] — start a new session",
            "/abort — stop the current task",
            "/skip — dismiss the waiting question (or deny the permission)",
          ].join("\n"),
        )
      case "status": {
        const id = activeSession ?? (await rootSessions())[0]?.id
        return send(
          [
            `📍 ${project} (${directory})`,
            id ? `Session: ${await title(id)} — ${busy.has(id) ? "working" : "idle"}` : "No session yet.",
            `Waiting for you: ${waiting.length}`,
          ].join("\n"),
        )
      }
      case "sessions": {
        const list = (await rootSessions()).slice(0, 8)
        listing = list.map((s) => s.id)
        if (!list.length) return send("No sessions yet.")
        return send(
          list
            .map((s, i) => `${i + 1}. ${s.title}${s.id === activeSession ? "  ← current" : ""}${busy.has(s.id) ? " (working)" : ""}`)
            .join("\n") + "\n\n/use N to switch",
        )
      }
      case "use": {
        const id = listing[Number(arg) - 1]
        if (!id) return send("Run /sessions first, then /use N.")
        activeSession = id
        await react(timestamp, "👍")
        return send(`Now using "${await title(id)}".`)
      }
      case "new": {
        const created = await client.session.create({ body: arg ? { title: arg } : {} })
        if (!created.data) throw new Error(`could not create a session: ${str(created.error)}`)
        activeSession = created.data.id
        return send(`🆕 New session "${created.data.title}". Your next message is its first prompt.`)
      }
      case "abort": {
        const id = activeSession
        if (!id || !busy.has(id)) return send("Nothing is running.")
        await client.session.abort({ path: { id } })
        return react(timestamp, "⏹️")
      }
      case "skip":
        return skipHead(timestamp)
      default:
        return send(`Unknown command /${name}. Send /help for the list.`)
    }
  }

  async function onSignalText(text: string, timestamp: number) {
    if (!text) return
    if (text.startsWith("/")) return command(text, timestamp)
    if (waiting.length) return answerHead(text, timestamp)
    return sendPrompt(text, timestamp)
  }

  let inbox = Promise.resolve()
  function onSignalEvent(data: string) {
    let msg: any
    try {
      msg = JSON.parse(data)
    } catch {
      return
    }
    if (msg?.account && msg.account !== account) return
    // Only messages you send yourself from another device (your phone) arrive as sync "sentMessage".
    const sent = msg?.envelope?.syncMessage?.sentMessage
    if (!sent || typeof sent.message !== "string" || typeof sent.timestamp !== "number") return
    const group = sent.groupInfo
    if (!group?.groupId) return
    if (groupId ? group.groupId !== groupId : group.groupName !== groupName) return
    if (!groupId) {
      groupId = group.groupId
      L(`using Signal group "${groupName}" (learned from an incoming message)`)
    }
    if (sentByUs.has(sent.timestamp)) return

    const text = sent.message.trim()
    L(`Signal message received (${text.startsWith("/") ? text.split(/\s/)[0] : `${text.length} chars`})`)
    inbox = inbox.then(() =>
      onSignalText(text, sent.timestamp).catch((e) => {
        L("handling a Signal message failed:", e)
        return send(`⚠️ ${str(e)}`)
      }),
    )
  }

  async function readEvents() {
    events = new AbortController()
    const res = await fetch(`${base}/api/v1/events`, {
      headers: { Accept: "text/event-stream" },
      signal: events.signal,
      // Bun-specific: no idle timeout on this long-lived stream.
      timeout: false,
    } as RequestInit)
    if (!res.ok || !res.body) throw new Error(`event stream: HTTP ${res.status}`)
    L("listening for Signal messages")
    const reader = res.body.getReader()
    const decoder = new TextDecoder()
    let buffer = ""
    while (!stopped) {
      const { value, done } = await reader.read()
      if (done) break
      buffer = (buffer + decoder.decode(value, { stream: true })).replace(/\r\n/g, "\n")
      let cut: number
      while ((cut = buffer.indexOf("\n\n")) >= 0) {
        const block = buffer.slice(0, cut)
        buffer = buffer.slice(cut + 2)
        const data = block
          .split("\n")
          .filter((line) => line.startsWith("data:"))
          .map((line) => line.slice(5).replace(/^ /, ""))
          .join("\n")
        if (data) onSignalEvent(data)
      }
    }
  }

  async function inboundLoop() {
    let announced = false
    while (!stopped) {
      if (!holdLock(token, directory)) {
        if (owner) L("another opencode now receives Signal messages")
        owner = false
        await sleep(15_000)
        continue
      }
      if (!owner) L("this opencode receives Signal messages")
      owner = true
      if (!(await daemonReady())) {
        await sleep(10_000)
        continue
      }
      try {
        await resolveGroup().catch((e) => L("listGroups failed:", e))
        if (!announced) {
          announced = true
          void send(`🟢 opencode connected — ${project}\n${directory}\nSend /help for commands.`)
        }
        await readEvents()
      } catch (e) {
        if (!stopped) L("event stream ended:", e)
      }
      if (!stopped) await sleep(3000)
    }
  }

  const heartbeat = setInterval(() => {
    if (owner && !holdLock(token, directory)) {
      owner = false
      events?.abort()
    }
  }, 15_000)

  void inboundLoop()

  // ---- opencode events ----

  async function onFinished(id: string) {
    const signalPrompt = fromSignal.delete(id)
    if (notify === "signal" && !signalPrompt) return

    const history = (await client.session.messages({ path: { id } })).data ?? []
    const lastUserIndex = history.map((m) => m.info.role).lastIndexOf("user")
    const replies = history.slice(lastUserIndex + 1).filter((m) => m.info.role === "assistant")
    let text = ""
    for (const m of [...replies].reverse()) {
      text = m.parts
        .filter((p: any) => p.type === "text" && !p.synthetic && !p.ignored)
        .map((p: any) => p.text)
        .join("\n\n")
        .trim()
      if (text) break
    }
    const error = (replies.at(-1)?.info as any)?.error
    const stoppedByUser = error?.name === "MessageAbortedError"
    const icon = stoppedByUser ? "⏹️" : error ? "⚠️" : "✅"
    const errorLine = error && !stoppedByUser ? `\n${error.name}: ${error.data?.message ?? ""}` : ""

    if (text.length > maxReply) text = `${text.slice(0, maxReply)}\n…(truncated — the full reply is in opencode)`
    const body = text ? `\n\n${text}` : error ? "" : "\n\n(finished without a text reply)"
    await send(`${icon} ${project} · ${await title(id)}${errorLine}${body}`)
    void buzz(stoppedByUser ? "opencode stopped" : error ? "opencode error" : "opencode finished", error ? "warning" : "white_check_mark")
  }

  return {
    event: async ({ event }) => {
      if (stopped) return
      const e = event as any
      const p = e.properties ?? {}
      try {
        switch (e.type) {
          case "session.created":
          case "session.updated":
            if (p.info?.id) sessions.set(p.info.id, { title: p.info.title, parentID: p.info.parentID })
            break
          case "session.deleted":
            sessions.delete(p.info?.id)
            if (activeSession === p.info?.id) activeSession = undefined
            break
          case "session.status": {
            const id = p.sessionID as string
            if (p.status?.type === "busy") {
              busy.add(id)
              if (!(await sessionInfo(id))?.parentID) activeSession = id
            } else if (p.status?.type === "idle" && busy.delete(id)) {
              if (!(await sessionInfo(id))?.parentID) await onFinished(id)
            }
            break
          }
          case "question.asked":
            if (opts.forwardQuestions === false) break
            addWaiting(
              { kind: "question", id: p.id, sessionID: p.sessionID, questions: p.questions ?? [], index: 0, answers: [], shown: false },
              "opencode has a question",
              "question",
            )
            break
          case "permission.asked":
            if (opts.forwardPermissions === false) break
            addWaiting(
              {
                kind: "permission",
                id: p.id,
                sessionID: p.sessionID,
                permission: p.permission,
                patterns: p.patterns ?? [],
                metadata: p.metadata ?? {},
                shown: false,
              },
              "opencode needs permission",
              "lock",
            )
            break
          case "question.replied":
          case "question.rejected":
          case "permission.replied":
            onAnsweredElsewhere(p.requestID)
            break
        }
      } catch (err) {
        L(`handling ${e.type} failed:`, err)
      }
    },

    dispose: async () => {
      stopped = true
      clearInterval(heartbeat)
      events?.abort()
      releaseLock(token)
      if (--liveInstances === 0) stopDaemon()
      L("disposed")
    },
  }
}

export default { id: "signal-bridge", server: SignalBridge }
