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
 * Several opencode windows (one plugin instance per project directory, in one process or many) share one Signal
 * group. Every window posts its own messages. One of them, the holder of `inbound.lock`, reads Signal and routes
 * each message to the window it is meant for (`routeOne`). Windows know about each other through small registry
 * files in STATE_DIR/instances/, and a message for another window is handed over through STATE_DIR/inbox/<token>/.
 *
 * Verified against opencode 1.18.34 and signal-cli 0.14.8.
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
const INSTANCES_DIR = path.join(STATE_DIR, "instances")
const INBOX_DIR = path.join(STATE_DIR, "inbox")
const FOCUS_FILE = path.join(STATE_DIR, "focus.json")
const LOCK_STALE_MS = 60_000
const BEAT_MS = 10_000
/** A window that has not taken a message handed to it within this long is treated as gone. */
const PICKUP_MS = 20_000
/**
 * Signal shows 2000 bytes of a message inline; signal-cli sends anything longer as a "Read more" attachment,
 * which the phone may show cut off. Longer texts are split into several messages below that size instead.
 */
const CHUNK_BYTES = 1800
/** Commands the routing window answers itself, whichever window you are talking to. */
const GLOBAL_COMMANDS = new Set(["help", "start", "projects", "project", "p"])

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

function readJson<T>(file: string): T | undefined {
  try {
    return JSON.parse(fs.readFileSync(file, "utf8"))
  } catch {
    return undefined
  }
}

/** Write through a rename, so a reader in another process never sees half a file. */
function writeJson(file: string, value: unknown) {
  fs.mkdirSync(path.dirname(file), { recursive: true })
  const tmp = `${file}.${process.pid}.${Math.random().toString(36).slice(2)}.tmp`
  fs.writeFileSync(tmp, JSON.stringify(value))
  fs.renameSync(tmp, file)
}

function pidAlive(pid: number) {
  try {
    process.kill(pid, 0)
    return true
  } catch (e: any) {
    return e?.code !== "ESRCH"
  }
}

const utf8Length = (s: string) => Buffer.byteLength(s, "utf8")

/** Split a text into pieces of at most `max` UTF-8 bytes, at line breaks where possible, numbered "(1/3)". */
function chunks(text: string, max = CHUNK_BYTES): string[] {
  if (utf8Length(text) <= max) return [text]
  const limit = max - 16 // room for the "\n(12/12)" marker
  const out: string[] = []
  let current: string | undefined
  for (const line of text.split("\n")) {
    const joined = current === undefined ? line : `${current}\n${line}`
    if (utf8Length(joined) <= limit) {
      current = joined
      continue
    }
    if (current !== undefined) out.push(current)
    current = undefined
    if (utf8Length(line) <= limit) {
      current = line
      continue
    }
    // One line longer than a whole piece: cut it between characters.
    let part = ""
    let bytes = 0
    for (const ch of line) {
      const size = utf8Length(ch)
      if (bytes + size > limit) {
        out.push(part)
        part = ""
        bytes = 0
      }
      part += ch
      bytes += size
    }
    current = part
  }
  if (current !== undefined && current !== "") out.push(current)
  return out.map((piece, i) => `${piece}\n(${i + 1}/${out.length})`)
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
// Inbound lock — every event-stream subscriber receives every message, so only one window reads them
// ---------------------------------------------------------------------------------------------------------------

type Lock = { pid: number; token: string; directory: string; time: number }

function readLock(): Lock | undefined {
  return readJson<Lock>(LOCK_FILE)
}

function lockStale(lock: Lock) {
  return Date.now() - lock.time > LOCK_STALE_MS || !pidAlive(lock.pid)
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
// Registry of open windows, and which one you are talking to
// ---------------------------------------------------------------------------------------------------------------

/** A waiting question or permission request, as other windows see it. */
type Summary = { id: string; kind: "question" | "permission"; at: number; root: string }

type Entry = {
  token: string
  pid: number
  project: string
  directory: string
  started: number
  beat: number
  /** When a prompt was last typed into this window, in its terminal or from Signal. */
  prompted: number
  /** The session Signal talks to in this window. */
  active?: string
  title?: string
  busy?: boolean
  waiting: Summary[]
  /** Signal timestamps of this window's recent messages: a swipe-reply to one of them is routed here. */
  sent: number[]
}

/**
 * `token`: the window you talk to — set by /project, by /new and /use, and by typing a prompt in a terminal.
 * `releasedAt`: requests that were already waiting then no longer take your next plain message (see `captures`).
 */
type Focus = { token?: string; at: number; releasedAt: number }

function liveEntries(): Entry[] {
  let names: string[]
  try {
    names = fs.readdirSync(INSTANCES_DIR)
  } catch {
    return []
  }
  const boot = Date.now() - os.uptime() * 1000
  const out: Entry[] = []
  for (const name of names) {
    if (!name.endsWith(".json")) continue
    const file = path.join(INSTANCES_DIR, name)
    const entry = readJson<Entry>(file)
    if (!entry) continue
    // A window removes its own entry when it closes; this catches the ones that were killed.
    if (!pidAlive(entry.pid) || entry.started < boot) {
      fs.rmSync(file, { force: true })
      fs.rmSync(path.join(INBOX_DIR, entry.token), { recursive: true, force: true })
      continue
    }
    out.push(entry)
  }
  return out
}

function readFocus(): Focus {
  return readJson<Focus>(FOCUS_FILE) ?? { at: 0, releasedAt: 0 }
}

function writeFocus(patch: Partial<Focus>) {
  try {
    writeJson(FOCUS_FILE, { ...readFocus(), ...patch })
  } catch (e) {
    log("focus", "write failed:", e)
  }
}

/** The window you are talking to: the one you picked, else the one you last typed a prompt in, else the newest. */
function focusOf(entries: Entry[], focus: Focus) {
  return (
    entries.find((e) => e.token === focus.token) ??
    [...entries].sort((a, b) => b.prompted - a.prompted || b.started - a.started)[0]
  )
}

/**
 * Whether a waiting request takes your next plain (non-reply) message. A request asked after your last
 * /new, /use or /project does; an older one only if it belongs to the session you are talking to.
 */
function captures(item: Summary, entry: Entry, focus: Focus, focused: Entry | undefined) {
  return item.at > focus.releasedAt || (entry.token === focused?.token && item.root === entry.active)
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

type Waiting = {
  id: string
  sessionID: string
  /** The top-level session the request belongs to (a subagent's request belongs to its parent's session). */
  root: string
  at: number
  shown: boolean
} & (
  | { kind: "question"; questions: QuestionInfo[]; answers: (string[] | undefined)[] }
  | { kind: "permission"; permission: string; patterns: string[]; metadata: Record<string, unknown> }
)
type QuestionItem = Extract<Waiting, { kind: "question" }>
type PermissionItem = Extract<Waiting, { kind: "permission" }>

function answerHint(q: QuestionInfo) {
  let hint = q.multiple ? "Reply with numbers, e.g. 1,3" : "Reply with a number"
  if (q.custom !== false) hint += " or type your own answer"
  return `${hint}. /skip to dismiss.`
}

/** One answer for one question, or the reason it was not understood. */
function parseAnswer(q: QuestionInfo, text: string): string[] | string {
  const t = text.trim()
  if (/^\d+([\s,]+\d+)*$/.test(t)) {
    const picks = [...new Set(t.split(/[\s,]+/).map(Number))]
    if (picks.some((n) => n < 1 || n > q.options.length)) return `pick a number from 1 to ${q.options.length}`
    if (!q.multiple && picks.length > 1) return "pick one option"
    return picks.map((n) => q.options[n - 1].label)
  }
  const exact = q.options.find((o) => o.label.toLowerCase() === t.toLowerCase())
  if (exact) return [exact.label]
  if (!t) return "the answer is empty"
  if (q.custom === false) return `pick a number from 1 to ${q.options.length}`
  return [t]
}

/**
 * Fill in the answers a reply gives. A request with several questions takes one line per open question, in
 * order, or lines tagged "Q2: …" for specific ones; with one question open, the whole reply is its answer.
 * Returns the problems found; every understood answer is kept even when another line was not.
 */
function fillAnswers(item: { questions: QuestionInfo[]; answers: (string[] | undefined)[] }, text: string) {
  const open = item.questions.map((_, i) => i).filter((i) => item.answers[i] === undefined)
  const problems: string[] = []
  if (!open.length) return problems
  const label = (i: number) => (item.questions.length > 1 ? `Q${i + 1}` : "your answer")
  const lines = text.split("\n").map((l) => l.trim()).filter(Boolean)
  const tags = lines.map((l) => /^q\s*(\d+)\s*[:.)\-]?\s*(.*)$/i.exec(l))

  let pairs: [number, string][] = []
  if (tags.some(Boolean)) {
    // Tagged lines; an untagged line continues the tagged line above it.
    for (const [k, line] of lines.entries()) {
      const tag = tags[k]
      if (tag) pairs.push([Number(tag[1]) - 1, tag[2]])
      else if (pairs.length) pairs[pairs.length - 1][1] += `\n${line}`
      else pairs.push([open[0], line])
    }
  } else if (open.length === 1) {
    pairs = [[open[0], text.trim()]]
  } else if (
    lines.length === 1 &&
    /^\d+([\s,]+\d+)+$/.test(lines[0]) &&
    !item.questions[open[0]].multiple &&
    lines[0].split(/[\s,]+/).length === open.length
  ) {
    // "2 1 3" — one number for each open question.
    pairs = lines[0].split(/[\s,]+/).map((n, k) => [open[k], n])
  } else if (lines.length <= open.length) {
    pairs = lines.map((line, k) => [open[k], line])
  } else {
    return [`I got ${lines.length} lines for ${open.length} questions. Send one line per question, or start lines with Q1:, Q2: …`]
  }

  for (const [i, value] of pairs) {
    const q = item.questions[i]
    if (!q) {
      problems.push(`there is no Q${i + 1}`)
      continue
    }
    const answer = parseAnswer(q, value)
    if (typeof answer === "string") problems.push(`${label(i)}: ${answer}`)
    else item.answers[i] = answer
  }
  return problems
}

// ---------------------------------------------------------------------------------------------------------------
// The plugin
// ---------------------------------------------------------------------------------------------------------------

/** A Signal message on its way to the window that handles it. `quote`: the message it replies to, if any. */
type Inbound = { text: string; timestamp: number; quote?: number }
/** What one of our Signal messages was about, so a reply to it reaches the right session or request. */
type About = { sessionID?: string; itemID?: string }

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
  const started = Date.now()
  // `opencode run` is a one-shot job: it posts like any window, but never reads Signal or takes the focus.
  const headless = process.argv.slice(2).find((a) => !a.startsWith("-")) === "run"

  let groupId = opts.groupId
  let stopped = false
  let owner = false
  let events: AbortController | undefined

  const sessions = new Map<string, { title: string; parentID?: string }>()
  const busy = new Set<string>()
  /** When each busy session's current run started; its final reply is what it wrote after that. */
  const runStart = new Map<string, number>()
  const fromSignal = new Set<string>()
  let activeSession: string | undefined
  let prompted = 0
  let listing: string[] = []
  /** User messages seen being created, until their text arrives (to tell a typed prompt from a compaction). */
  const newUserMessages = new Map<string, string>()
  const seenUserMessages = new Set<string>()

  const waiting: Waiting[] = []
  const answeredHere = new Set<string>()
  const about = new Map<number, About>()

  liveInstances++
  L(`enabled (signal-cli at ${base}, group "${groupName}"${headless ? ", opencode run: outbound only" : ""})`)

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

  async function rootOf(id: string) {
    let current = id
    for (let i = 0; i < 8; i++) {
      const parent = (await sessionInfo(current).catch(() => undefined))?.parentID
      if (!parent) break
      current = parent
    }
    return current
  }

  async function rootSessions() {
    const res = await client.session.list()
    return (res.data ?? []).filter((s) => !s.parentID).sort((a, b) => b.time.updated - a.time.updated)
  }

  /** Show the session in the terminal too, so the window on screen matches what Signal talks to. */
  function showInTerminal(id: string) {
    void api("post", "/tui/select-session", { body: { sessionID: id } }).catch((e) =>
      L("could not switch the terminal to the session:", e),
    )
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

  async function sendOne(text: string): Promise<number | undefined> {
    try {
      if (!(await daemonReady())) throw new Error(`signal-cli daemon is not reachable at ${base}`)
      const gid = await resolveGroup()
      if (!gid) throw new Error(`no Signal group named "${groupName}" yet — send a message in it from your phone`)
      const result = await rpc("send", { groupId: gid, message: text })
      return typeof result?.timestamp === "number" ? result.timestamp : 0
    } catch (e) {
      L("send failed:", e)
      return undefined
    }
  }

  function remember(timestamp: number, info: About) {
    if (!timestamp) return
    about.set(timestamp, info)
    if (about.size > 300) about.delete(about.keys().next().value!)
    publishSoon()
  }

  /**
   * Everything this window sends to Signal — messages and reactions — goes through one queue. A message takes
   * its place in the queue when `post` is called, not when its text is ready: opencode does not wait for one
   * event handler before calling the next, so building texts in parallel would otherwise reorder them.
   */
  let outbox = Promise.resolve()
  function enqueue(job: () => Promise<void>) {
    const next = outbox.then(job, job)
    outbox = next.catch(() => {})
    return outbox
  }

  function post(text: string | undefined | Promise<string | undefined>, info: About | (() => About) = {}) {
    const ready = Promise.resolve(text).catch((e) => {
      L("building a message failed:", e)
      return undefined
    })
    return enqueue(async () => {
      const body = await ready
      if (!body || stopped) return
      for (const piece of chunks(body)) {
        const timestamp = await sendOne(piece)
        if (timestamp === undefined) break
        remember(timestamp, typeof info === "function" ? info() : info)
      }
    })
  }

  function react(timestamp: number, emoji: string) {
    return enqueue(async () => {
      if (stopped || !groupId) return
      await rpc("sendReaction", { groupId, emoji, targetAuthor: account, targetTimestamp: timestamp }).catch((e) =>
        L("reaction failed:", e),
      )
    })
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

  // ---- this window in the registry ----

  function selfEntry(): Entry {
    return {
      token,
      pid: process.pid,
      project,
      directory,
      started,
      beat: Date.now(),
      prompted,
      active: activeSession,
      title: activeSession ? sessions.get(activeSession)?.title : undefined,
      busy: activeSession ? busy.has(activeSession) : undefined,
      waiting: waiting.map((w) => ({ id: w.id, kind: w.kind, at: w.at, root: w.root })),
      sent: [...about.keys()],
    }
  }

  let publishTimer: ReturnType<typeof setTimeout> | undefined
  function publish() {
    clearTimeout(publishTimer)
    publishTimer = undefined
    if (headless || stopped) return
    try {
      writeJson(path.join(INSTANCES_DIR, `${token}.json`), selfEntry())
    } catch (e) {
      L("registry write failed:", e)
    }
  }

  function publishSoon() {
    if (!publishTimer && !headless && !stopped) publishTimer = setTimeout(publish, 100)
  }

  /** Every open window, this one as it is right now. */
  function windows() {
    const others = liveEntries().filter((e) => e.token !== token)
    return [...others, ...(headless ? [] : [selfEntry()])].sort((a, b) => a.started - b.started)
  }

  function noteActivity(sessionID: string) {
    activeSession = sessionID
    prompted = Date.now()
    if (!headless) writeFocus({ token, at: prompted })
    publishSoon()
  }

  // ---- waiting questions / permissions ----

  async function questionText(item: QuestionItem, intro?: string) {
    const open = item.questions.map((_, i) => i).filter((i) => item.answers[i] === undefined)
    const several = item.questions.length > 1
    const lines = [`❓ ${project} · ${await title(item.root)}`]
    if (intro) lines.push(intro)
    if (several && open.length === item.questions.length) lines.push(`${item.questions.length} questions:`)
    for (const i of open) {
      const q = item.questions[i]
      lines.push("", several ? `Q${i + 1} · ${q.header}` : q.header, q.question)
      q.options.forEach((o, k) => lines.push(`  ${k + 1}. ${o.label}${o.description ? ` — ${o.description}` : ""}`))
      if (q.multiple) lines.push("  (one or more)")
    }
    lines.push("")
    if (open.length > 1) {
      const example = open
        .slice(0, 3)
        .map((i, k) => {
          const n = item.questions[i].options.length
          return n ? (item.questions[i].multiple && n > 1 ? "1,2" : String((k % n) + 1)) : "your answer"
        })
      lines.push(
        "Reply with one line per question, in order, e.g.",
        ...example,
        "",
        `Numbers pick options, other text is your own answer. Answer some now and the rest later, or start a line with Q${open[1] + 1}: to answer just that one. /skip to dismiss.`,
      )
    } else lines.push(answerHint(item.questions[open[0]]))
    return lines.join("\n")
  }

  async function permissionText(item: PermissionItem) {
    const detail = [item.metadata?.command, item.metadata?.filepath, item.metadata?.url]
      .filter((v) => typeof v === "string")
      .slice(0, 1)
    const patterns = item.patterns.slice(0, 5).map((p) => `  ${p}`)
    return [
      `🔐 ${project} · ${await title(item.root)}`,
      `opencode wants permission: ${item.permission}`,
      ...detail.map((d) => `  ${String(d).slice(0, 500)}`),
      ...(detail.length ? [] : patterns),
      "",
      "Reply y = allow once · a = always allow · n = deny",
      "(or type a reason to deny with feedback)",
    ].join("\n")
  }

  function showHead() {
    const item = waiting[0]
    if (!item || item.shown) return
    item.shown = true
    const text = (async () => {
      const body = item.kind === "question" ? await questionText(item) : await permissionText(item)
      const more = waiting.length > 1 ? `\n\n(+${waiting.length - 1} more waiting here)` : ""
      const elsewhere = windows().some((e) => e.token !== token && e.waiting.length)
      const hint = elsewhere ? "\n↩️ Other windows are waiting too — swipe-reply to this message to answer this one." : ""
      return body + more + hint
    })()
    post(text, () => ({ itemID: item.id, sessionID: item.root }))
  }

  function dropWaiting(id: string) {
    const index = waiting.findIndex((w) => w.id === id)
    if (index >= 0) waiting.splice(index, 1)
    showHead()
    publishSoon()
  }

  async function answer(item: Waiting, text: string, timestamp: number) {
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
      react(timestamp, reply === "reject" ? "🚫" : "👍")
      dropWaiting(item.id)
      return
    }

    const before = item.answers.filter(Boolean).length
    const problems = fillAnswers(item, t)
    const open = item.questions.filter((_, i) => item.answers[i] === undefined).length
    if (item.answers.filter(Boolean).length > before) react(timestamp, "👍")
    if (open) {
      const intro = problems.length
        ? `Didn't catch ${problems.join("; ")}.`
        : `Got it. ${open} more to go:`
      post(questionText(item, intro), { itemID: item.id, sessionID: item.root })
      return
    }
    answeredHere.add(item.id)
    await api("post", "/question/{requestID}/reply", { path: { requestID: item.id }, body: { answers: item.answers } })
    dropWaiting(item.id)
  }

  async function skip(item: Waiting | undefined, timestamp: number) {
    if (!item) return post(`${project}: nothing is waiting for an answer.`)
    answeredHere.add(item.id)
    if (item.kind === "question") {
      await api("post", "/question/{requestID}/reject", { path: { requestID: item.id } })
    } else {
      waiting.filter((w) => w.kind === "permission" && w.sessionID === item.sessionID).forEach((w) => answeredHere.add(w.id))
      await api("post", "/permission/{requestID}/reply", { path: { requestID: item.id }, body: { reply: "reject" } })
    }
    react(timestamp, "👌")
    dropWaiting(item.id)
  }

  function onAnsweredElsewhere(requestID: string) {
    const index = waiting.findIndex((w) => w.id === requestID)
    if (answeredHere.delete(requestID) || index < 0) {
      if (index >= 0) dropWaiting(requestID)
      return
    }
    const [item] = waiting.splice(index, 1)
    if (item.shown) post(`✔️ ${project}: answered in opencode (${item.kind === "question" ? "question" : item.permission}).`)
    showHead()
    publishSoon()
  }

  function addWaiting(item: Waiting, heading: string, tags: string) {
    waiting.push(item)
    void rootOf(item.sessionID).then((root) => {
      item.root = root
      publishSoon()
    })
    showHead()
    publishSoon()
    void buzz(heading, tags)
  }

  /** The waiting request your next plain message answers here, if any. */
  function heldHere() {
    const focus = readFocus()
    const all = windows()
    const me = all.find((e) => e.token === token)
    if (!me) return undefined
    const focused = focusOf(all, focus)
    return waiting.find((w) => captures({ id: w.id, kind: w.kind, at: w.at, root: w.root }, me, focus, focused))
  }

  // ---- prompts and commands from Signal ----

  async function sendPrompt(text: string, timestamp: number, target?: string) {
    let id = target ?? activeSession
    let guessed = false
    if (!id) {
      id = (await rootSessions())[0]?.id
      guessed = !!id
    }
    if (!id) {
      const created = await client.session.create({ body: {} })
      if (!created.data) throw new Error(`could not create a session: ${str(created.error)}`)
      id = created.data.id
    }

    // Reuse the agent and model of the session's last prompt so Signal prompts behave like the terminal ones.
    const history = (await client.session.messages({ path: { id }, query: { limit: 50 } })).data ?? []
    const lastUser = [...history].reverse().find((m) => m.info.role === "user")?.info as any
    const lastReply = [...history].reverse().find((m) => m.info.role === "assistant")?.info as any
    const body: any = { parts: [{ type: "text", text }] }
    if (lastUser?.agent) body.agent = lastUser.agent
    else if (lastReply?.mode) body.agent = lastReply.mode
    if (lastUser?.model?.providerID) body.model = { providerID: lastUser.model.providerID, modelID: lastUser.model.modelID }
    else if (lastReply?.providerID) body.model = { providerID: lastReply.providerID, modelID: lastReply.modelID }

    // Read this before prompting: the prompt itself marks the session busy before promptAsync returns.
    const wasBusy = busy.has(id)
    const res = await client.session.promptAsync({ path: { id }, body })
    if (res.error) throw new Error(`prompt failed: ${str(res.error)}`)

    noteActivity(id)
    fromSignal.add(id)
    react(timestamp, "👀")
    const name = await title(id)
    if (wasBusy) post(`📨 ${project} · Queued for "${name}" — it is still working on the previous task.`, { sessionID: id })
    else if (guessed) post(`📨 ${project} · Sent to "${name}", the most recent session. /sessions to pick another.`, { sessionID: id })
    void client.tui
      .showToast({ body: { title: "Signal", message: `Prompt from Signal → ${name}`, variant: "info" } })
      .catch(() => {})
  }

  function helpText() {
    return [
      "opencode over Signal",
      "Type anything to send it as a prompt to the current session.",
      "When a question or permission request is waiting, your next message answers it.",
      "Swipe-reply to any message to answer that request, or to prompt that session.",
      "",
      "/status — what is running",
      "/sessions — recent sessions",
      "/use N — switch to session N from /sessions",
      "/new [title] — start a new session",
      "/abort — stop the current task",
      "/skip — dismiss the waiting question (or deny the permission)",
      "/projects — open opencode windows · /project N — switch to one",
    ].join("\n")
  }

  function projectList(all: Entry[], focused: Entry | undefined) {
    return [
      "Open opencode windows:",
      ...all.map(
        (e, i) =>
          `${i + 1}. ${e.project}${e.title ? ` · ${e.title}` : ""}${e.busy ? " (working)" : ""}` +
          `${e.waiting.length ? ` — ${e.waiting.length} waiting` : ""}${e.token === focused?.token ? "  ← current" : ""}`,
      ),
      "",
      "/project N to switch",
    ].join("\n")
  }

  /** /help, /projects and /project N — the routing window answers these for all windows. */
  async function globalCommand(name: string, arg: string, timestamp: number) {
    if (name === "help" || name === "start") return post(helpText())
    const all = windows()
    const focus = readFocus()
    const focused = focusOf(all, focus)
    if (name === "projects" || !arg) return post(projectList(all, focused))
    const n = Number(arg)
    const pick = Number.isInteger(n) && n >= 1 ? all[n - 1] : all.find((e) => e.project.toLowerCase() === arg.toLowerCase())
    if (!pick) return post(`No window "${arg}".\n\n${projectList(all, focused)}`)
    writeFocus({ token: pick.token, at: Date.now(), releasedAt: Date.now() })
    react(timestamp, "👍")
    return post(
      [
        `➡️ Now talking to ${pick.project}${pick.title ? ` · ${pick.title}` : ""}${pick.busy ? " (working)" : ""}`,
        pick.directory,
        ...(pick.waiting.length ? [`${pick.waiting.length} request(s) waiting there — swipe-reply to one to answer it.`] : []),
      ].join("\n"),
    )
  }

  async function command(text: string, timestamp: number, quoted: Waiting | undefined) {
    const [first, ...rest] = text.slice(1).split(/\s+/)
    const name = first.toLowerCase()
    const arg = rest.join(" ").trim()
    if (GLOBAL_COMMANDS.has(name)) return globalCommand(name, arg, timestamp)
    switch (name) {
      case "status": {
        const id = activeSession ?? (await rootSessions())[0]?.id
        const others = windows().filter((e) => e.token !== token)
        return post(
          [
            `📍 ${project} (${directory})`,
            id ? `Session: ${await title(id)} — ${busy.has(id) ? "working" : "idle"}` : "No session yet.",
            `Waiting for you: ${waiting.length}`,
            ...(others.length
              ? ["", `Also open: ${others.map((e) => `${e.project}${e.busy ? " (working)" : ""}`).join(", ")} — /projects`]
              : []),
          ].join("\n"),
        )
      }
      case "sessions": {
        const list = (await rootSessions()).slice(0, 8)
        listing = list.map((s) => s.id)
        if (!list.length) return post(`${project}: no sessions yet.`)
        return post(
          `${project} sessions:\n` +
            list
              .map((s, i) => `${i + 1}. ${s.title}${s.id === activeSession ? "  ← current" : ""}${busy.has(s.id) ? " (working)" : ""}`)
              .join("\n") +
            "\n\n/use N to switch",
        )
      }
      case "use": {
        const id = listing[Number(arg) - 1]
        if (!id) return post("Run /sessions first, then /use N.")
        activeSession = id
        publishSoon()
        showInTerminal(id)
        react(timestamp, "👍")
        return post(`${project} · Now using "${await title(id)}".`, { sessionID: id })
      }
      case "new":
      case "clear": {
        const created = await client.session.create({ body: arg ? { title: arg } : {} })
        if (!created.data) throw new Error(`could not create a session: ${str(created.error)}`)
        const id = created.data.id
        sessions.set(id, { title: created.data.title })
        activeSession = id
        publishSoon()
        showInTerminal(id)
        const left = waiting.length
          ? `\n(${waiting.length} request(s) still waiting in other sessions — swipe-reply to one to answer it.)`
          : ""
        return post(`🆕 ${project} · New session "${created.data.title}". Your next message is its first prompt.${left}`, {
          sessionID: id,
        })
      }
      case "abort": {
        const id = activeSession ?? (await rootSessions())[0]?.id
        if (!id || !busy.has(id)) return post(`${project}: nothing is running${id ? ` in "${await title(id)}"` : ""}.`)
        await client.session.abort({ path: { id } })
        return react(timestamp, "⏹️")
      }
      case "skip":
        return skip(quoted ?? waiting[0], timestamp)
      default:
        return post(`Unknown command /${first}. Send /help for the list.`)
    }
  }

  async function onSignalText(msg: Inbound) {
    const text = msg.text
    if (!text) return
    const replyTo = msg.quote !== undefined ? about.get(msg.quote) : undefined
    const quoted = replyTo?.itemID ? waiting.find((w) => w.id === replyTo.itemID) : undefined
    if (text.startsWith("/")) return command(text, msg.timestamp, quoted)
    if (quoted) return answer(quoted, text, msg.timestamp)
    if (replyTo?.sessionID) return sendPrompt(text, msg.timestamp, replyTo.sessionID)
    const held = heldHere()
    if (held) return answer(held, text, msg.timestamp)
    return sendPrompt(text, msg.timestamp)
  }

  let inbox = Promise.resolve()
  function accept(msg: Inbound) {
    const what = msg.text.startsWith("/") ? msg.text.split(/\s/)[0] : `${msg.text.length} chars`
    L(`Signal message received (${what}${msg.quote !== undefined ? ", a reply" : ""})`)
    inbox = inbox.then(() =>
      onSignalText(msg).catch((e) => {
        L("handling a Signal message failed:", e)
        return post(`⚠️ ${project}: ${str(e)}`)
      }),
    )
  }

  // ---- routing (only the window holding inbound.lock) ----

  const handedOver: { file: string; token: string; project: string; due: number }[] = []
  let handSeq = 0

  function handOver(target: Entry, msg: Inbound) {
    const file = path.join(INBOX_DIR, target.token, `${String(msg.timestamp).padStart(15, "0")}-${process.pid}-${++handSeq}.json`)
    try {
      writeJson(file, msg)
    } catch (e) {
      L(`could not hand a message to ${target.project}:`, e)
      post(`⚠️ Could not pass your message to ${target.project}: ${str(e)}`)
      return
    }
    handedOver.push({ file, token: target.token, project: target.project, due: Date.now() + PICKUP_MS })
    L(`routed a Signal message to ${target.project}`)
  }

  function checkHandovers() {
    for (let i = handedOver.length - 1; i >= 0; i--) {
      const h = handedOver[i]
      if (!fs.existsSync(h.file)) handedOver.splice(i, 1)
      else if (Date.now() > h.due) {
        handedOver.splice(i, 1)
        fs.rmSync(h.file, { force: true })
        // It is not reading its inbox; forget the window until it writes its entry again.
        fs.rmSync(path.join(INSTANCES_DIR, `${h.token}.json`), { force: true })
        L(`${h.project} did not pick up a routed message`)
        post(`⚠️ ${h.project} did not pick up your message — is that opencode window still open? Send it again; /projects shows what is open.`)
      }
    }
  }

  async function routeOne(msg: Inbound) {
    const all = windows()
    const focus = readFocus()
    const focused = focusOf(all, focus)
    const [first, ...rest] = msg.text.startsWith("/") ? msg.text.slice(1).split(/\s+/) : []
    const name = first?.toLowerCase()
    if (name && GLOBAL_COMMANDS.has(name)) {
      L(`Signal message received (/${name})`)
      return globalCommand(name, rest.join(" ").trim(), msg.timestamp)
    }

    let target = msg.quote !== undefined ? all.find((e) => e.sent.includes(msg.quote!)) : undefined
    if (!target && (!name || name === "skip")) {
      const held = all
        .flatMap((e) => e.waiting.filter((w) => captures(w, e, focus, focused)).map((w) => ({ e, w })))
        .sort((a, b) => a.w.at - b.w.at)
      target = held[0]?.e
    }
    target ??= focused
    if (!target) return accept(msg)
    if (name === "new" || name === "clear" || name === "use") {
      // Decided here, before the window handles it, so the next message is routed by the new choice.
      const now = Date.now()
      writeFocus({ token: target.token, at: now, releasedAt: now })
    }
    if (target.token === token) accept(msg)
    else handOver(target, msg)
  }

  let routing = Promise.resolve()
  let clockWarnedAt = 0
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

    // Signal may sort messages by the time their sender stamped on them. If this computer's clock is behind
    // Signal's, replies can appear above the message they answer.
    const delivered = msg.envelope.serverDeliveredTimestamp
    if (typeof delivered === "number") {
      const behind = delivered - Date.now()
      if (behind > 3000) {
        L(`this computer's clock is ${(behind / 1000).toFixed(1)}s behind Signal's`)
        if (Date.now() - clockWarnedAt > 6 * 3600_000) {
          clockWarnedAt = Date.now()
          post(
            `⚠️ This computer's clock is ${Math.round(behind / 1000)}s behind Signal's, so replies may show up above your messages. ` +
              "On WSL: `sudo hwclock -s`, or `wsl --shutdown` from Windows.",
          )
        }
      }
    }

    const inbound: Inbound = { text: sent.message.trim(), timestamp: sent.timestamp }
    if (typeof sent.quote?.id === "number") inbound.quote = sent.quote.id
    routing = routing.then(() => routeOne(inbound)).catch((e) => L("routing a Signal message failed:", e))
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
    while (!stopped) {
      if (!holdLock(token, directory)) {
        if (owner) L("another opencode now reads Signal")
        owner = false
        await sleep(BEAT_MS)
        continue
      }
      if (!owner) L("this opencode reads Signal and routes messages")
      owner = true
      if (!(await daemonReady())) {
        await sleep(BEAT_MS)
        continue
      }
      try {
        await resolveGroup().catch((e) => L("listGroups failed:", e))
        await readEvents()
      } catch (e) {
        if (!stopped) L("event stream ended:", e)
      }
      if (!stopped) await sleep(3000)
    }
  }

  // ---- messages handed over by the routing window ----

  function pollInbox() {
    if (stopped) return
    const dir = path.join(INBOX_DIR, token)
    let names: string[]
    try {
      names = fs.readdirSync(dir).filter((n) => n.endsWith(".json")).sort()
    } catch {
      return
    }
    for (const name of names) {
      const file = path.join(dir, name)
      const msg = readJson<Inbound>(file)
      fs.rmSync(file, { force: true })
      if (msg && typeof msg.text === "string") accept(msg)
    }
  }

  const timers: ReturnType<typeof setInterval>[] = []
  let announce: ReturnType<typeof setTimeout> | undefined
  if (!headless) {
    publish()
    timers.push(
      setInterval(() => {
        publish()
        if (owner && !holdLock(token, directory)) {
          owner = false
          events?.abort()
        }
        if (owner) checkHandovers()
      }, BEAT_MS),
      setInterval(pollInbox, 500),
    )
    void inboundLoop()
    // Announce after a moment, so the short-lived instances opencode starts for some commands stay quiet.
    announce = setTimeout(() => {
      const count = windows().length
      post(
        `🟢 opencode connected — ${project}\n${directory}\n` +
          (count > 1 ? `${count} windows open: /projects to switch.` : "Send /help for commands."),
      )
    }, 3000)
  }

  // ---- opencode events ----

  async function finishedText(id: string, start: number | undefined, end: number) {
    if ((await sessionInfo(id))?.parentID) return undefined
    const history = (await client.session.messages({ path: { id }, query: { limit: 80 } })).data ?? []
    type Message = (typeof history)[number]
    // A prompt someone typed — not the user messages compaction and its "continue" create.
    const typed = (m: Message) =>
      m.info.role === "user" && m.parts.some((p: any) => p.type === "text" && !p.synthetic && !p.ignored)
    // This run's messages only: a prompt sent right after the session went idle is the next run's business.
    let run = history.filter((m) => m.info.time.created <= end)
    if (start !== undefined) run = run.filter((m) => m.info.time.created >= start - 1000)
    else {
      let last = run.length - 1
      while (last >= 0 && !typed(run[last])) last--
      run = run.slice(Math.max(last, 0))
    }
    // One answer per prompt: prompts queued while it was working each get theirs. A reply belongs to the prompt
    // it answers (its parentID); the user messages compaction creates count as part of the prompt before them.
    const groups = new Map<string, Message[]>()
    const promptOf = new Map<string, string>()
    let current = ""
    for (const m of run) {
      if (m.info.role === "user") {
        if (typed(m)) current = m.info.id
        promptOf.set(m.info.id, current)
      } else if (m.info.role === "assistant" && !(m.info as any).summary) {
        const key = promptOf.get((m.info as any).parentID) ?? current
        groups.set(key, [...(groups.get(key) ?? []), m])
      }
    }
    const replies = run.filter((m) => m.info.role === "assistant" && !(m.info as any).summary)
    const texts: string[] = []
    for (const group of groups.values()) {
      for (const m of [...group].reverse()) {
        const text = m.parts
          .filter((p: any) => p.type === "text" && !p.synthetic && !p.ignored)
          .map((p: any) => p.text)
          .join("\n\n")
          .trim()
        if (text) {
          texts.push(text)
          break
        }
      }
    }
    let text = texts.join("\n\n― ― ―\n\n")
    const error = (replies.at(-1)?.info as any)?.error
    const stoppedByUser = error?.name === "MessageAbortedError"
    const icon = stoppedByUser ? "⏹️" : error ? "⚠️" : "✅"
    const errorLine = error && !stoppedByUser ? `\n${error.name}: ${error.data?.message ?? ""}` : ""

    if (text.length > maxReply) text = `${text.slice(0, maxReply)}\n…(truncated — the full reply is in opencode)`
    const body = text ? `\n\n${text}` : error ? "" : "\n\n(finished without a text reply)"
    void buzz(stoppedByUser ? "opencode stopped" : error ? "opencode error" : "opencode finished", error ? "warning" : "white_check_mark")
    return `${icon} ${project} · ${await title(id)}${errorLine}${body}`
  }

  function onFinished(id: string, start: number | undefined, end: number) {
    const signalPrompt = fromSignal.delete(id)
    if (notify === "signal" && !signalPrompt) return
    post(finishedText(id, start, end), { sessionID: id })
  }

  return {
    // opencode calls this for every event without waiting for the previous call to finish: everything up to the
    // first `await` runs in event order, so messages are queued (`post`) before awaiting anything.
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
            if (p.status?.type === "idle") {
              if (busy.delete(id)) {
                const start = runStart.get(id)
                runStart.delete(id)
                onFinished(id, start, Date.now())
                publishSoon()
              }
            } else if (!busy.has(id)) {
              busy.add(id)
              runStart.set(id, Date.now())
              publishSoon()
            }
            break
          }
          // A prompt typed in the terminal makes its session the one Signal talks to. A user message only counts
          // once real text arrives for it: compaction and similar bookkeeping also create user messages.
          case "message.updated": {
            const info = p.info
            if (info?.role !== "user" || seenUserMessages.has(info.id)) break
            seenUserMessages.add(info.id)
            if (seenUserMessages.size > 500) seenUserMessages.delete(seenUserMessages.values().next().value!)
            if (Date.now() - (info.time?.created ?? 0) < 60_000) newUserMessages.set(info.id, info.sessionID)
            break
          }
          case "message.part.updated": {
            const part = p.part
            if (part?.type !== "text" || part.synthetic || !newUserMessages.has(part.messageID)) break
            const sessionID = newUserMessages.get(part.messageID)!
            newUserMessages.delete(part.messageID)
            if (!(await sessionInfo(sessionID))?.parentID) noteActivity(sessionID)
            break
          }
          case "question.asked":
            if (opts.forwardQuestions === false) break
            addWaiting(
              {
                kind: "question",
                id: p.id,
                sessionID: p.sessionID,
                root: p.sessionID,
                at: Date.now(),
                questions: p.questions ?? [],
                answers: [],
                shown: false,
              },
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
                root: p.sessionID,
                at: Date.now(),
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
      timers.forEach((t) => clearInterval(t))
      clearTimeout(announce)
      clearTimeout(publishTimer)
      events?.abort()
      releaseLock(token)
      if (!headless) {
        fs.rmSync(path.join(INSTANCES_DIR, `${token}.json`), { force: true })
        fs.rmSync(path.join(INBOX_DIR, token), { recursive: true, force: true })
      }
      if (--liveInstances === 0) stopDaemon()
      L("disposed")
    },
  }
}

export default { id: "signal-bridge", server: SignalBridge }
