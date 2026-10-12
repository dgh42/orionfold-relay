/**
 * Concurrency control for on-demand agent runs.
 *
 * "On-demand" is every task run whose `source_type` is not `scheduled` or
 * `heartbeat` — a manual execute, a resume, a chat `execute_task`, a handoff,
 * a workflow step. Schedule firings keep their own independent cap and their
 * own claim in `@/lib/schedules/slot-claim`; the two counts are disjoint and
 * together cover every running task, so neither can starve the other.
 *
 * Three pieces:
 *   - an atomic single-statement claim (never check-then-act — see
 *     `tdr-atomic-slot-claim.md`),
 *   - a FIFO waiting line for runs the cap deferred, drained when a slot frees
 *     and by a low-frequency timer that only exists while something is waiting,
 *   - a startup sweep for runs a previous process left behind as `running`.
 *
 * An over-cap run is never failed: its row stays `queued` and it starts when
 * its turn comes. Callers that await the run simply wait longer.
 */

import { sqlite } from "@/lib/db";
import { readIntConfig } from "@/lib/schedules/config";
import { SETTINGS_KEYS } from "@/lib/constants/settings";

/** Source types the schedule cap owns. Everything else is on-demand. */
const SCHEDULED_SOURCE_TYPES = ["scheduled", "heartbeat"] as const;

const DEFAULT_TASK_MAX_CONCURRENT = 20;

/**
 * How often the waiting line is re-examined while something is in it. A slot
 * normally frees through `releaseTaskSlot()`; this timer is the backstop for
 * the ways a run can stop without passing through the dispatcher (a cancel, a
 * reap, a row edited by hand). It only exists while the line is non-empty and
 * is unref'd, so an idle Relay pays nothing for it.
 */
const DRAIN_INTERVAL_MS = 5_000;

/** Statuses a first claim may start from. Mirrors what `startTaskExecution`
 * has always been willing to run: anything that is not already finished. */
const CLAIMABLE_STATUSES = ["queued", "planned", "failed", "cancelled"] as const;

type ClaimableStatus = (typeof CLAIMABLE_STATUSES)[number];

/**
 * The on-demand half of the running-task count, expressed the same way in the
 * claim and in the counter so the two can never disagree.
 */
const ON_DEMAND_PREDICATE = `(source_type IS NULL OR source_type NOT IN ('${SCHEDULED_SOURCE_TYPES.join(
  "', '",
)}'))`;

/**
 * Atomic on-demand slot claim.
 *
 * One statement, for the reason `tdr-atomic-slot-claim.md` gives: every start
 * path can run concurrently, so a SELECT followed by an UPDATE would let two
 * callers both read "one slot left" and both take it. The subquery inside the
 * WHERE clause makes SQLite evaluate the count under the same write lock as
 * the update.
 *
 * Deliberately writes no lease. An on-demand run has no bounded duration to
 * check against, so a lease could only ever kill a long legitimate run; the
 * orphan case is handled once at startup instead (`sweepOrphanedTaskRuns`).
 *
 * Four status placeholders rather than a built list, so the statement is
 * prepared once; a caller wanting fewer repeats one of them.
 */
export const TASK_SLOT_CLAIM_SQL =
  `UPDATE tasks SET status = 'running', slot_claimed_at = ?, updated_at = ? WHERE id = ? AND status IN (?, ?, ?, ?) AND ${ON_DEMAND_PREDICATE} AND (SELECT COUNT(*) FROM tasks WHERE status = 'running' AND ${ON_DEMAND_PREDICATE}) < ?`;

const claimStmt = sqlite.prepare(TASK_SLOT_CLAIM_SQL);

const countRunningStmt = sqlite.prepare(
  `SELECT COUNT(*) AS n FROM tasks WHERE status = 'running' AND ${ON_DEMAND_PREDICATE}`,
);

const readStatusStmt = sqlite.prepare(
  "SELECT status FROM tasks WHERE id = ?",
);

const parkStmt = sqlite.prepare(
  "UPDATE tasks SET status = 'queued', updated_at = ? WHERE id = ? AND status = ?",
);

const sweepStmt = sqlite.prepare(
  `UPDATE tasks SET status = 'failed', failure_reason = 'process_restart', result = COALESCE(result, ?), updated_at = ? WHERE status = 'running' AND ${ON_DEMAND_PREDICATE} RETURNING id`,
);

/** True for every task run the on-demand cap governs. */
export function isOnDemandSourceType(
  sourceType: string | null | undefined,
): boolean {
  if (!sourceType) return true;
  return !(SCHEDULED_SOURCE_TYPES as readonly string[]).includes(sourceType);
}

/**
 * The on-demand concurrency cap: `TASK_MAX_CONCURRENT`, then the
 * `task.maxConcurrent` setting, then 20.
 *
 * Independent of `SCHEDULE_MAX_CONCURRENT` by design. The schedule cap is low
 * (2) because a schedule firing is unattended and arrives in bursts; an
 * on-demand run is something a person or a workflow is waiting on, so its cap
 * is set high enough to be invisible in normal use and only bites when a burst
 * would otherwise exhaust the host.
 */
export function getTaskMaxConcurrent(): number {
  return readIntConfig(
    "TASK_MAX_CONCURRENT",
    SETTINGS_KEYS.TASK_MAX_CONCURRENT,
    DEFAULT_TASK_MAX_CONCURRENT,
  );
}

/** Running on-demand tasks. The schedule cap counts the complement. */
export function countRunningTaskSlots(): number {
  const row = countRunningStmt.get() as { n: number } | undefined;
  return row?.n ?? 0;
}

function readTaskStatus(taskId: string): string | undefined {
  const row = readStatusStmt.get(taskId) as { status: string } | undefined;
  return row?.status;
}

/**
 * Try to take a slot for `taskId` now.
 *
 * @param options.from — statuses the claim may start from. Defaults to every
 *   non-terminal one; the waiting line passes `["queued"]` so a run cancelled
 *   while it waited is not resurrected when its turn comes.
 * @param options.force — bypass the cap. The caller is responsible for the
 *   audit record; see the `force` branch of the task execute route.
 * @returns true when this call transitioned the row to `running`.
 */
export function claimTaskSlot(
  taskId: string,
  options?: { from?: readonly ClaimableStatus[]; force?: boolean; cap?: number },
): boolean {
  const from = options?.from ?? CLAIMABLE_STATUSES;
  if (from.length === 0 || from.length > CLAIMABLE_STATUSES.length) {
    throw new Error(
      `claimTaskSlot: from must name 1..${CLAIMABLE_STATUSES.length} statuses`,
    );
  }
  // Pad to the statement's four placeholders by repeating the first entry — a
  // repeated value in an IN list is the same set.
  const padded = [...from];
  while (padded.length < 4) padded.push(from[0]);

  const cap = options?.force
    ? Number.MAX_SAFE_INTEGER
    : (options?.cap ?? getTaskMaxConcurrent());

  // Drizzle integer({ mode: "timestamp" }) stores Unix seconds; Math.ceil so a
  // slotClaimedAt read back is never before the Date.now() taken just here.
  const nowSec = Math.ceil(Date.now() / 1000);
  const result = claimStmt.run(
    nowSec,
    nowSec,
    taskId,
    padded[0],
    padded[1],
    padded[2],
    padded[3],
    cap,
  );
  return result.changes === 1;
}

interface Waiter {
  taskId: string;
  resolve: (claimed: boolean) => void;
}

/**
 * The waiting line, in submission order.
 *
 * In memory rather than in the table, and that is the load-bearing decision:
 * `queued` is a resting state here — a task a person created and has not run
 * yet sits in it indefinitely — so a drain that walked every queued row would
 * launch runs nobody asked for. Only a run that was actually submitted and
 * deferred by the cap is in this list.
 *
 * The cost is that a restart empties the line. That leaves those rows `queued`,
 * which is exactly where they would have been had the process died a moment
 * earlier, and a person can run them again. Nothing is marked failed.
 */
const waiting: Waiter[] = [];
let drainTimer: ReturnType<typeof setInterval> | null = null;
let draining = false;

function syncDrainTimer(): void {
  if (waiting.length > 0 && drainTimer === null) {
    drainTimer = setInterval(() => {
      drainTaskQueue();
    }, DRAIN_INTERVAL_MS);
    drainTimer.unref?.();
    return;
  }
  if (waiting.length === 0 && drainTimer !== null) {
    clearInterval(drainTimer);
    drainTimer = null;
  }
}

/**
 * Start as many waiting runs as the cap allows, oldest submission first.
 *
 * Strictly head-of-line: when the head cannot claim because the cap is full we
 * stop rather than letting a later arrival overtake it. That is what FIFO
 * means, and it is what keeps a workflow's first branch from being starved by
 * its own later ones.
 */
export function drainTaskQueue(): void {
  if (draining) return;
  draining = true;
  try {
    while (waiting.length > 0) {
      const head = waiting[0];
      const status = readTaskStatus(head.taskId);
      if (status !== "queued") {
        // Deleted, cancelled, or started by another path while it waited.
        waiting.shift();
        head.resolve(false);
        continue;
      }
      if (!claimTaskSlot(head.taskId, { from: ["queued"] })) break;
      waiting.shift();
      head.resolve(true);
    }
  } finally {
    draining = false;
    syncDrainTimer();
  }
}

/**
 * Signal that a slot may have freed. Called when an on-demand run settles.
 *
 * Deliberately a signal and not a decrement: the cap is counted from the table
 * on every claim, so a missed call can delay a queued run until the drain timer
 * but can never let the cap be exceeded.
 */
export function releaseTaskSlot(): void {
  drainTaskQueue();
}

/** How many runs are waiting for a slot. For tests and telemetry. */
export function countWaitingTaskSlots(): number {
  return waiting.length;
}

function enqueueForTaskSlot(taskId: string): Promise<boolean> {
  return new Promise<boolean>((resolve) => {
    waiting.push({ taskId, resolve });
    syncDrainTimer();
    // A slot may have freed between the failed claim and this push.
    drainTaskQueue();
  });
}

/**
 * Hold the dispatcher until this run owns a slot.
 *
 * Returns "claimed" when the row is `running` and counted, "abandoned" when
 * there is nothing left to run (the row was deleted, cancelled, or finished
 * while it waited). Never throws and never fails a run because the cap is full.
 */
export async function acquireTaskSlot(
  taskId: string,
): Promise<"claimed" | "abandoned"> {
  if (claimTaskSlot(taskId)) return "claimed";

  const status = readTaskStatus(taskId);
  if (status === undefined) return "abandoned";
  // Already running — the caller (a route, or a retry inside one run) claimed
  // it, so the slot is held and the dispatcher owns its release.
  if (status === "running") return "claimed";
  if (!(CLAIMABLE_STATUSES as readonly string[]).includes(status)) {
    return "abandoned";
  }

  // The cap is full. Park the run as `queued` so what it is waiting for is
  // visible, then take a place in line.
  if (status !== "queued") {
    parkStmt.run(Math.ceil(Date.now() / 1000), taskId, status);
  }
  return (await enqueueForTaskSlot(taskId)) ? "claimed" : "abandoned";
}

const SWEEP_RESULT =
  "Relay restarted while this run was in flight. The attempt was stopped and was not replayed automatically.";

/**
 * Mark on-demand runs a previous process left `running` as failed.
 *
 * Run once at boot, before anything can start a new run. Scheduled runs are
 * untouched: they carry a lease and `reapExpiredLeases` is theirs.
 *
 * This is the whole of the orphan story for on-demand runs, on purpose. A
 * periodic reaper would need a duration to measure against, and an on-demand
 * run has none — a reaper would eventually kill a long legitimate run instead
 * of a dead one. A row still `running` after a restart, by contrast, is
 * unambiguously dead: nothing in this process is executing it.
 *
 * @returns the ids it failed.
 */
export function sweepOrphanedTaskRuns(): string[] {
  const nowSec = Math.ceil(Date.now() / 1000);
  const rows = sweepStmt.all(SWEEP_RESULT, nowSec) as Array<{ id: string }>;
  return rows.map((row) => row.id);
}

/** Test seam: drop the waiting line and stop the drain timer. */
export function __resetTaskSlotsForTests(): void {
  while (waiting.length > 0) {
    waiting.shift()?.resolve(false);
  }
  syncDrainTimer();
}
