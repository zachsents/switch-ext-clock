// Stopwatches with laps, and countdown timers that chime when they run out. Wall-clock times, so
// they keep time while the app is closed.
import { readFileSync } from "node:fs"
import { Context, Effect, Layer, Queue, Ref, Schema } from "effect"
import {
  AppEvents,
  BASIC_CATALOG,
  defineApp,
  defineEvent,
  SWITCH_CATALOG,
  tool,
  writeFileAtomic,
} from "switch-ext-kit"

import { parseDuration } from "./duration.ts"

const clock = Schema.Struct({
  id: Schema.Finite,
  name: Schema.String,
  kind: Schema.Literals(["timer", "stopwatch"]),
  /** A timer's full length; 0 for a stopwatch. */
  durationMs: Schema.Finite,
  /** Time counted before the current run started. */
  bankedMs: Schema.Finite,
  runningSince: Schema.NullOr(Schema.Finite),
  /** A timer that ran out and hasn't been dismissed. */
  ringing: Schema.Boolean,
  /** A stopwatch's total at each lap, oldest first. */
  laps: Schema.Array(Schema.Finite),
})
type Clock = typeof clock.Type

export const Changed = defineEvent(
  "clock.changed",
  1,
  Schema.Struct({
    change: Schema.Literals(["created", "started", "paused", "lapped", "reset", "rang", "deleted"]),
    clock,
  }),
)
type Change = (typeof Changed.payload.Type)["change"]

/** Every this often, as its manifest's `heartbeat` promises. */
const BEAT = 5 * 60_000

/** How it is, beaten by the alarms' layer, so it stops when that does. */
export const Heartbeat = defineEvent(
  "clock.heartbeat",
  1,
  Schema.Struct({
    entity: Schema.Literal("sense:clock"),
    summary: Schema.String,
    facts: Schema.Struct({
      failing: Schema.NullOr(Schema.String),
      clocks: Schema.Finite,
      running: Schema.Finite,
      ringing: Schema.Finite,
    }),
  }),
)

/** One change to a clock: what it becomes (null: deleted) and what to call it, or null for none. */
type Step = (c: Clock, now: number) => readonly [Clock | null, Change] | null

const state = Schema.Struct({ next: Schema.Finite, clocks: Schema.Array(clock) })
type State = typeof state.Type
const decodeState = Schema.decodeUnknownSync(Schema.fromJsonString(state))

const run = (c: Clock, now: number) => (c.runningSince === null ? 0 : now - c.runningSince)
const elapsed = (c: Clock, now: number) => c.bankedMs + run(c, now)
const remaining = (c: Clock, now: number) => Math.max(0, c.durationMs - elapsed(c, now))
const reset = (c: Clock): Clock => ({
  ...c,
  bankedMs: 0,
  runningSince: null,
  ringing: false,
  laps: [],
})

const view = (c: Clock, now: number) =>
  c.kind === "timer"
    ? {
        id: c.id,
        name: c.name,
        kind: c.kind,
        durationMs: c.durationMs,
        remainingMs: remaining(c, now),
        running: c.runningSince !== null,
        ringing: c.ringing,
        endsAt:
          c.runningSince === null
            ? null
            : new Date(c.runningSince + c.durationMs - c.bankedMs).toISOString(),
      }
    : {
        id: c.id,
        name: c.name,
        kind: c.kind,
        elapsedMs: elapsed(c, now),
        running: c.runningSince !== null,
        laps: c.laps,
      }

// "Pasta" taken: "Pasta 2", as a file would be.
const freeName = (clocks: readonly Clock[], base: string) => {
  const taken = new Set(clocks.map((c) => c.name))
  if (!taken.has(base)) {
    return base
  }
  let n = 2
  while (taken.has(`${base} ${n}`)) {
    n += 1
  }
  return `${base} ${n}`
}

const chime = () => {
  if (process.platform === "darwin") {
    Bun.spawn(["afplay", "/System/Library/Sounds/Glass.aiff"], {
      stdout: "ignore",
      stderr: "ignore",
    })
  }
}

/** The clocks, saved to a file after every change. */
export class Clocks extends Context.Service<
  Clocks,
  {
    readonly read: Effect.Effect<State>
    /** Applies `f` to one clock, or fails if there's none with that id. */
    readonly change: (id: number, step: Step) => Effect.Effect<Clock, string>
    readonly add: (
      make: (id: number, clocks: readonly Clock[], now: number) => Clock,
    ) => Effect.Effect<Clock>
  }
>()("switch/clock/Clocks") {
  static readonly layer = (file: string) =>
    Layer.effect(
      Clocks,
      Effect.gen(function* making() {
        const events = yield* AppEvents
        const loaded = yield* Effect.sync((): State => {
          try {
            return decodeState(readFileSync(file, "utf-8"))
          } catch {
            return { next: 1, clocks: [] }
          }
        })
        const ref = yield* Ref.make(loaded)
        const wake = yield* Queue.unbounded<true>()

        const commit = (next: State, changed: Clock, change: Change) =>
          Effect.gen(function* committing() {
            yield* Ref.set(ref, next)
            yield* Effect.sync(() => {
              writeFileAtomic(file, JSON.stringify(next))
            })
            yield* Effect.orDie(events.emit(Changed.make({ change, clock: changed })))
            yield* Queue.offer(wake, true)
          })

        const change = (id: number, step: Step) =>
          Effect.gen(function* changing() {
            const s = yield* Ref.get(ref)
            const found = s.clocks.find((c) => c.id === id)
            if (!found) {
              return yield* Effect.fail(`No clock ${id}`)
            }
            const result = step(found, Date.now())
            if (result === null) {
              return found
            }
            const [after, what] = result
            const clocks =
              after === null
                ? s.clocks.filter((c) => c.id !== id)
                : s.clocks.map((c) => (c.id === id ? after : c))
            yield* commit({ ...s, clocks }, after ?? found, what)
            return after ?? found
          })

        const add = (make: (id: number, clocks: readonly Clock[], now: number) => Clock) =>
          Effect.gen(function* adding() {
            const s = yield* Ref.get(ref)
            const made = make(s.next, s.clocks, Date.now())
            yield* commit({ next: s.next + 1, clocks: [...s.clocks, made] }, made, "created")
            return made
          })

        // Alarms: ring each timer that ran out (ones that did while closed, at once), then sleep
        // until the next one ends or something changes.
        yield* Effect.forkScoped(
          Effect.forever(
            Effect.gen(function* ringing() {
              const now = Date.now()
              const s = yield* Ref.get(ref)
              const due = s.clocks.filter(
                (c) => c.kind === "timer" && c.runningSince !== null && remaining(c, now) === 0,
              )
              for (const c of due) {
                yield* change(c.id, (x) => [
                  { ...x, runningSince: null, bankedMs: x.durationMs, ringing: true },
                  "rang",
                ])
                chime()
              }
              const ends = (yield* Ref.get(ref)).clocks
                .filter((c) => c.kind === "timer" && c.runningSince !== null)
                .map((c) => remaining(c, Date.now()))
              const take = Queue.take(wake)
              yield* ends.length === 0 ? take : Effect.timeoutOption(take, Math.min(...ends))
              yield* Queue.clear(wake)
            }).pipe(Effect.catch(Effect.logWarning)),
          ),
        )

        const beat = Effect.gen(function* beating() {
          const { clocks } = yield* Ref.get(ref)
          const running = clocks.filter((c) => c.runningSince !== null).length
          const ringing = clocks.filter((c) => c.ringing).length
          yield* events.emit(
            Heartbeat.make({
              entity: "sense:clock",
              summary: `${clocks.length} clocks, ${running} running`,
              facts: { failing: null, clocks: clocks.length, running, ringing },
            }),
          )
        })
        yield* Effect.forkScoped(
          Effect.forever(
            Effect.andThen(Effect.ignore(beat, { log: "Warn" }), Effect.sleep(`${BEAT} millis`)),
          ),
        )

        return { read: Ref.get(ref), change, add }
      }),
    )
}

const id = Schema.Struct({ id: Schema.Finite })

/** A tool that changes one clock and answers with it. */
const changing = (name: string, description: string, step: Step) =>
  tool(name, { description, input: id }, (input) =>
    Effect.gen(function* stepping() {
      const clocks = yield* Clocks
      const after = yield* clocks.change(input.id, step)
      return view(after, Date.now())
    }),
  )

const stateTool = tool(
  "state",
  { description: "Every stopwatch and timer, with time left or elapsed.", readOnly: true },
  () =>
    Effect.gen(function* reading() {
      const s = yield* (yield* Clocks).read
      const now = Date.now()
      return { asOf: now, clocks: s.clocks.map((c) => view(c, now)) }
    }),
)

const create = tool(
  "new",
  {
    description:
      'Add a timer or stopwatch and start it. A timer\'s duration is plain words: "5 minutes", "1:30", "1h 30m", "until 3pm"; a bare number is minutes.',
    input: Schema.Struct({
      kind: Schema.optionalKey(Schema.Literals(["timer", "stopwatch"])),
      duration: Schema.optionalKey(Schema.String),
      name: Schema.optionalKey(Schema.String),
    }),
  },
  (input) =>
    Effect.gen(function* creating() {
      const clocks = yield* Clocks
      const kind = input.kind ?? "timer"
      const length = kind === "timer" ? parseDuration(input.duration ?? "5") : 0
      if (length === null || (kind === "timer" && length <= 0)) {
        return yield* Effect.fail(`Can't read duration "${input.duration ?? ""}"`)
      }
      const made = yield* clocks.add((next, all, now) => ({
        id: next,
        name: freeName(all, input.name ?? (kind === "timer" ? "Timer" : "Stopwatch")),
        kind,
        durationMs: length,
        bankedMs: 0,
        runningSince: now,
        ringing: false,
        laps: [],
      }))
      return view(made, Date.now())
    }),
)

const toggle = changing(
  "toggle",
  "Start or pause a stopwatch or timer. A ringing timer is dismissed instead.",
  (c, now) => {
    if (c.ringing) {
      return [reset(c), "reset"]
    }
    if (c.runningSince === null) {
      return [{ ...c, runningSince: now }, "started"]
    }
    const banked = elapsed(c, now)
    const bankedMs = c.kind === "timer" ? Math.min(banked, c.durationMs) : banked
    return [{ ...c, runningSince: null, bankedMs }, "paused"]
  },
)

const lap = changing("lap", "Record a lap on a running stopwatch.", (c, now) =>
  c.kind === "stopwatch" && c.runningSince !== null
    ? [{ ...c, laps: [...c.laps, elapsed(c, now)] }, "lapped"]
    : null,
)

const resetTool = changing(
  "reset",
  "Stop and set back: a stopwatch to zero with no laps, a timer to its full length, silenced.",
  (c) => [reset(c), "reset"],
)

const remove = changing("delete", "Delete a stopwatch or timer.", () => [null, "deleted"])

export const clockApp = defineApp<Clocks>({
  name: "clock",
  version: "1",
  instructions: "Stopwatches and timers on this computer. A timer chimes when it runs out.",
  tools: [stateTool, create, toggle, lap, resetTool, remove],
  events: [Changed, Heartbeat],
  widgets: [
    {
      name: "timers",
      title: "Timers",
      surfaces: ["space"],
      resource: "ui://clock/timers",
      size: {
        aspect: "auto",
        min: { cols: 3, rows: 1 },
        preferred: { cols: 4, rows: 2 },
        grows: true,
      },
      tool: "state",
      refresh: [Changed.type],
      // A line per clock, Switch's Line in a List from A2UI's basic catalog: its mark, its name, its
      // time running live.
      a2ui: () => [
        { version: "1.0", createSurface: { surfaceId: "timers", catalogId: SWITCH_CATALOG } },
        {
          version: "1.0",
          updateComponents: {
            surfaceId: "timers",
            components: [
              {
                id: "root",
                component: "List",
                catalogId: BASIC_CATALOG,
                children: { componentId: "line", path: "/clocks" },
              },
              {
                id: "line",
                component: "Line",
                label: { "@path": "name" },
                running: { "@path": "running" },
                ringing: { "@path": "ringing" },
                left: { "@path": "remainingMs" },
                elapsed: { "@path": "elapsedMs" },
                asOf: { "@path": "/asOf" },
              },
            ],
          },
        },
      ],
    },
  ],
})
