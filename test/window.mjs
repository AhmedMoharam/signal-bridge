// One opencode window for the tests: the real plugin, a fake opencode client, driven by run.mjs over IPC.
// Events are delivered the way opencode delivers them — the hook is called and NOT awaited.

const pluginPath = process.env.BRIDGE_PLUGIN
const directory = process.env.BRIDGE_DIR
const options = JSON.parse(process.env.BRIDGE_OPTIONS)

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const delays = { messages: 0 }
const sessions = new Map()
const messages = new Map()
const calls = []
let seq = 0
const newId = (prefix) => `${prefix}_${process.pid}_${String(++seq).padStart(5, "0")}`
let hooks

const emit = (type, properties) => {
  hooks.event({ event: { type, properties } })
}

function addSession({ title, parentID } = {}) {
  const now = Date.now()
  const s = { id: newId("ses"), title: title ?? `New session - ${new Date(now).toISOString()}`, parentID, time: { created: now, updated: now } }
  sessions.set(s.id, s)
  messages.set(s.id, [])
  emit("session.created", { info: s })
  return s
}

function addMessage(sessionID, info, parts) {
  const id = newId("msg")
  const m = {
    info: { id, sessionID, time: { created: Date.now() }, ...info },
    parts: parts.map((p) => ({ id: newId("prt"), sessionID, messageID: id, ...p })),
  }
  messages.get(sessionID).push(m)
  sessions.get(sessionID).time.updated = Date.now()
  return m
}

function userMessage(sessionID, text, { synthetic, compaction } = {}) {
  const parts = compaction ? [{ type: "compaction" }] : [{ type: "text", text, ...(synthetic ? { synthetic: true } : {}) }]
  const m = addMessage(sessionID, { role: "user", agent: "build", model: { providerID: "prov", modelID: "mod" } }, parts)
  emit("message.updated", { info: m.info })
  for (const part of m.parts) emit("message.part.updated", { part })
  return m
}

function assistant(sessionID, text, { parentID, summary, error } = {}) {
  const parent = parentID ?? [...messages.get(sessionID)].reverse().find((m) => m.info.role === "user")?.info.id
  const m = addMessage(
    sessionID,
    { role: "assistant", parentID: parent, providerID: "prov", modelID: "mod", mode: "build", ...(summary ? { summary: true } : {}), ...(error ? { error } : {}) },
    text ? [{ type: "text", text }] : [],
  )
  emit("message.updated", { info: m.info })
  return m
}

const status = (sessionID, type) => emit("session.status", { sessionID, status: { type } })

const client = {
  session: {
    list: async () => ({ data: [...sessions.values()] }),
    get: async ({ path: { id } }) => (sessions.has(id) ? { data: sessions.get(id) } : { error: "not found" }),
    create: async ({ body } = {}) => ({ data: addSession(body ?? {}) }),
    messages: async ({ path: { id }, query }) => {
      if (delays.messages) await sleep(delays.messages)
      const all = messages.get(id) ?? []
      return { data: query?.limit ? all.slice(-query.limit) : all }
    },
    promptAsync: async ({ path: { id }, body }) => {
      calls.push({ type: "prompt", sessionID: id, text: body.parts[0].text, agent: body.agent, model: body.model })
      userMessage(id, body.parts[0].text)
      status(id, "busy")
      return { data: true }
    },
    abort: async ({ path: { id } }) => {
      calls.push({ type: "abort", sessionID: id })
      return { data: true }
    },
  },
  tui: {
    showToast: async () => ({ data: true }),
  },
  _client: {
    get: async () => ({ data: [] }),
    post: async ({ url, path, body }) => {
      calls.push({ type: "post", url, path, body })
      const id = path?.requestID
      // Like opencode: answering a request announces it.
      if (url === "/question/{requestID}/reply") setTimeout(() => emit("question.replied", { requestID: id, sessionID: "" }), 5)
      if (url === "/question/{requestID}/reject") setTimeout(() => emit("question.rejected", { requestID: id, sessionID: "" }), 5)
      if (url === "/permission/{requestID}/reply") setTimeout(() => emit("permission.replied", { requestID: id, sessionID: "" }), 5)
      return { data: true }
    },
  },
}

const commands = {
  async start() {
    const mod = await import(pluginPath)
    const plugin = mod.default.server ?? mod.default
    hooks = await plugin({ client, directory, worktree: directory, project: {}, serverUrl: new URL("http://127.0.0.1:1"), $: undefined }, options)
    return { pid: process.pid }
  },
  session: (a) => addSession(a),
  // A prompt typed in the terminal.
  type: ({ sessionID, text }) => {
    const m = userMessage(sessionID, text)
    status(sessionID, "busy")
    return m.info.id
  },
  user: ({ sessionID, text, synthetic, compaction }) => userMessage(sessionID, text, { synthetic, compaction }).info.id,
  reply: ({ sessionID, text, parentID, summary, error }) => assistant(sessionID, text, { parentID, summary, error }).info.id,
  status: ({ sessionID, type }) => status(sessionID, type),
  question: ({ sessionID, questions }) => {
    const id = newId("que")
    emit("question.asked", { id, sessionID, questions })
    return id
  },
  permission: ({ sessionID, permission, patterns, metadata }) => {
    const id = newId("per")
    emit("permission.asked", { id, sessionID, permission, patterns: patterns ?? [], metadata: metadata ?? {} })
    return id
  },
  answeredInTerminal: ({ requestID, kind }) => emit(kind === "permission" ? "permission.replied" : "question.replied", { requestID, sessionID: "" }),
  delay: ({ messages: ms }) => {
    delays.messages = ms
  },
  state: () => ({ calls }),
  dispose: async () => {
    await hooks.dispose?.()
    return true
  },
}

process.on("message", async ({ id, cmd, args }) => {
  try {
    const result = await commands[cmd](args ?? {})
    process.send({ id, result: result ?? null })
  } catch (e) {
    process.send({ id, error: String(e?.stack ?? e) })
  }
})
