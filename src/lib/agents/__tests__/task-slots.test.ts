/** @vitest-environment node */

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { randomUUID } from "crypto";
import { Worker } from "node:worker_threads";
import { join } from "node:path";
import { eq } from "drizzle-orm";
import { db } from "@/lib/db";
import { settings, tasks } from "@/lib/db/schema";
import { SETTINGS_KEYS } from "@/lib/constants/settings";
import {
  claimSlot,
  countRunningScheduledSlots,
} from "@/lib/schedules/slot-claim";
import {
  TASK_SLOT_CLAIM_SQL,
  __resetTaskSlotsForTests,
  acquireTaskSlot,
  claimTaskSlot,
  countRunningTaskSlots,
  countWaitingTaskSlots,
  getTaskMaxConcurrent,
  isOnDemandSourceType,
  releaseTaskSlot,
  sweepOrphanedTaskRuns,
} from "../task-slots";

type SourceType = typeof tasks.$inferInsert.sourceType;

function seedTask(
  overrides: Partial<typeof tasks.$inferInsert> = {},
): string {
  const id = randomUUID();
  const now = new Date();
  db.insert(tasks)
    .values({
      id,
      title: `task-${id.slice(0, 4)}`,
      status: "queued",
      priority: 2,
      resumeCount: 0,
      createdAt: now,
      updatedAt: now,
      ...overrides,
    })
    .run();
  return id;
}

function statusOf(id: string): string | undefined {
  return db.select().from(tasks).where(eq(tasks.id, id)).get()?.status;
}

function settle(id: string, status: "completed" | "cancelled" = "completed") {
  db.update(tasks).set({ status, updatedAt: new Date() }).where(eq(tasks.id, id)).run();
  releaseTaskSlot();
}

beforeEach(() => {
  __resetTaskSlotsForTests();
  db.delete(tasks).run();
  db.delete(settings).where(eq(settings.key, SETTINGS_KEYS.TASK_MAX_CONCURRENT)).run();
});

afterEach(() => {
  __resetTaskSlotsForTests();
  vi.unstubAllEnvs();
});

describe("isOnDemandSourceType", () => {
  it.each<[SourceType, boolean]>([
    ["scheduled", false],
    ["heartbeat", false],
    ["manual", true],
    ["workflow", true],
    [null, true],
    [undefined, true],
  ])("treats %s as on-demand=%s", (sourceType, expected) => {
    expect(isOnDemandSourceType(sourceType)).toBe(expected);
  });
});

describe("getTaskMaxConcurrent", () => {
  it("defaults to 20", () => {
    expect(getTaskMaxConcurrent()).toBe(20);
  });

  it("prefers the environment variable over the setting", () => {
    db.insert(settings)
      .values({
        key: SETTINGS_KEYS.TASK_MAX_CONCURRENT,
        value: "7",
        updatedAt: new Date(),
      })
      .run();
    expect(getTaskMaxConcurrent()).toBe(7);

    vi.stubEnv("TASK_MAX_CONCURRENT", "3");
    expect(getTaskMaxConcurrent()).toBe(3);
  });

  it("falls back to the default when the environment value is not a positive integer", () => {
    vi.stubEnv("TASK_MAX_CONCURRENT", "nonsense");
    expect(getTaskMaxConcurrent()).toBe(20);
  });
});

describe("claimTaskSlot", () => {
  it("is a single SQL statement", () => {
    expect(TASK_SLOT_CLAIM_SQL.trim().split(";").filter(Boolean)).toHaveLength(1);
    expect(TASK_SLOT_CLAIM_SQL).toMatch(/^UPDATE tasks SET status = 'running'/);
  });

  it("claims a queued on-demand task and records the claim time", () => {
    const id = seedTask({ sourceType: "manual" });

    expect(claimTaskSlot(id)).toBe(true);

    const row = db.select().from(tasks).where(eq(tasks.id, id)).get();
    expect(row?.status).toBe("running");
    expect(row?.slotClaimedAt).not.toBeNull();
    // No lease: an on-demand run has no bounded duration to reap against.
    expect(row?.leaseExpiresAt).toBeNull();
  });

  it("treats a NULL source_type as on-demand", () => {
    const id = seedTask({ sourceType: null });
    expect(claimTaskSlot(id)).toBe(true);
    expect(countRunningTaskSlots()).toBe(1);
  });

  it("refuses over the cap and leaves the row queued", () => {
    vi.stubEnv("TASK_MAX_CONCURRENT", "2");
    const first = seedTask({ sourceType: "manual" });
    const second = seedTask({ sourceType: "workflow" });
    const third = seedTask({ sourceType: "manual" });

    expect(claimTaskSlot(first)).toBe(true);
    expect(claimTaskSlot(second)).toBe(true);
    expect(claimTaskSlot(third)).toBe(false);

    expect(statusOf(third)).toBe("queued");
    expect(countRunningTaskSlots()).toBe(2);
  });

  it("refuses a repeated claim for the same task", () => {
    const id = seedTask({ sourceType: "manual" });
    expect(claimTaskSlot(id)).toBe(true);
    expect(claimTaskSlot(id)).toBe(false);
  });

  it("honors a restricted from-status list", () => {
    const id = seedTask({ sourceType: "manual", status: "failed" });

    expect(claimTaskSlot(id, { from: ["queued"] })).toBe(false);
    expect(claimTaskSlot(id, { from: ["failed", "cancelled"] })).toBe(true);
  });

  it("never claims a scheduled or heartbeat task", () => {
    const scheduled = seedTask({ sourceType: "scheduled" });
    const heartbeat = seedTask({ sourceType: "heartbeat" });

    expect(claimTaskSlot(scheduled)).toBe(false);
    expect(claimTaskSlot(heartbeat)).toBe(false);
  });

  it("force bypasses a full cap", () => {
    vi.stubEnv("TASK_MAX_CONCURRENT", "1");
    const running = seedTask({ sourceType: "manual" });
    const forced = seedTask({ sourceType: "manual" });

    expect(claimTaskSlot(running)).toBe(true);
    expect(claimTaskSlot(forced)).toBe(false);
    expect(claimTaskSlot(forced, { force: true })).toBe(true);
    expect(countRunningTaskSlots()).toBe(2);
  });

  it("allows exactly one winner when two SQLite connections race beneath cap 1", async () => {
    const firstTaskId = seedTask({ sourceType: "manual" });
    const secondTaskId = seedTask({ sourceType: "manual" });
    const databasePath = join(process.env.RELAY_DATA_DIR!, "relay.db");
    const startBarrier = new SharedArrayBuffer(Int32Array.BYTES_PER_ELEMENT);
    const startSignal = new Int32Array(startBarrier);

    const workerSource = `
      const { parentPort, workerData } = require("node:worker_threads");
      const Database = require("better-sqlite3");
      const db = new Database(workerData.databasePath);
      db.pragma("busy_timeout = 5000");
      const start = new Int32Array(workerData.startBarrier);
      parentPort.postMessage({ type: "ready" });
      Atomics.wait(start, 0, 0);
      try {
        const result = db.prepare(workerData.sql).run(
          workerData.nowSec,
          workerData.nowSec,
          workerData.taskId,
          "queued",
          "queued",
          "queued",
          "queued",
          1,
        );
        parentPort.postMessage({ type: "result", changes: result.changes });
      } catch (error) {
        parentPort.postMessage({
          type: "error",
          message: error instanceof Error ? error.message : String(error),
        });
      } finally {
        db.close();
      }
    `;

    const runClaim = (taskId: string) => {
      let markReady!: () => void;
      const ready = new Promise<void>((resolve) => {
        markReady = resolve;
      });
      const result = new Promise<number>((resolve, reject) => {
        const worker = new Worker(workerSource, {
          eval: true,
          workerData: {
            databasePath,
            sql: TASK_SLOT_CLAIM_SQL,
            taskId,
            nowSec: Math.ceil(Date.now() / 1000),
            startBarrier,
          },
        });
        worker.on("message", (message) => {
          if (message.type === "ready") {
            markReady();
            return;
          }
          if (message.type === "error") {
            reject(new Error(`Competing claim failed: ${message.message}`));
            return;
          }
          resolve(message.changes);
        });
        worker.on("error", reject);
        worker.on("exit", (code) => {
          if (code !== 0) reject(new Error(`Competing claim worker exited ${code}`));
        });
      });
      return { ready, result };
    };

    const firstClaim = runClaim(firstTaskId);
    const secondClaim = runClaim(secondTaskId);
    await Promise.all([firstClaim.ready, secondClaim.ready]);
    Atomics.store(startSignal, 0, 1);
    Atomics.notify(startSignal, 0, 2);

    const changes = await Promise.all([firstClaim.result, secondClaim.result]);
    expect(changes.sort()).toEqual([0, 1]);
    expect(countRunningTaskSlots()).toBe(1);
  });
});

describe("the two caps are independent", () => {
  it("a full on-demand cap does not block a schedule firing", () => {
    vi.stubEnv("TASK_MAX_CONCURRENT", "1");
    const onDemand = seedTask({ sourceType: "manual" });
    const scheduled = seedTask({ sourceType: "scheduled" });

    expect(claimTaskSlot(onDemand)).toBe(true);
    expect(claimSlot(scheduled, 2, 1200).claimed).toBe(true);

    expect(countRunningTaskSlots()).toBe(1);
    expect(countRunningScheduledSlots()).toBe(1);
  });

  it("a full schedule cap does not block an on-demand run", () => {
    const scheduled = seedTask({ sourceType: "scheduled" });
    const heartbeat = seedTask({ sourceType: "heartbeat" });
    const onDemand = seedTask({ sourceType: "manual" });

    expect(claimSlot(scheduled, 2, 1200).claimed).toBe(true);
    expect(claimSlot(heartbeat, 2, 1200).claimed).toBe(true);
    expect(countRunningScheduledSlots()).toBe(2);

    expect(claimTaskSlot(onDemand)).toBe(true);
    expect(countRunningTaskSlots()).toBe(1);
  });
});

describe("acquireTaskSlot", () => {
  it("queues over-cap runs and starts them in submission order", async () => {
    vi.stubEnv("TASK_MAX_CONCURRENT", "1");
    const running = seedTask({ sourceType: "manual" });
    expect(claimTaskSlot(running)).toBe(true);

    const first = seedTask({ sourceType: "manual" });
    const second = seedTask({ sourceType: "manual" });
    const third = seedTask({ sourceType: "manual" });

    const started: string[] = [];
    const waiters = [first, second, third].map((id) =>
      acquireTaskSlot(id).then((outcome) => {
        started.push(id);
        return outcome;
      }),
    );
    // Let the three enqueue before any slot frees.
    await Promise.resolve();
    expect(countWaitingTaskSlots()).toBe(3);
    expect([first, second, third].map(statusOf)).toEqual([
      "queued",
      "queued",
      "queued",
    ]);

    settle(running);
    expect(await waiters[0]).toBe("claimed");
    expect(started).toEqual([first]);
    expect(statusOf(first)).toBe("running");
    expect(statusOf(second)).toBe("queued");

    settle(first);
    expect(await waiters[1]).toBe("claimed");
    expect(started).toEqual([first, second]);

    settle(second);
    expect(await waiters[2]).toBe("claimed");
    expect(started).toEqual([first, second, third]);
    expect(countWaitingTaskSlots()).toBe(0);
  });

  it("claims immediately when a slot is free", async () => {
    const id = seedTask({ sourceType: "manual" });
    await expect(acquireTaskSlot(id)).resolves.toBe("claimed");
    expect(statusOf(id)).toBe("running");
  });

  it("reports a row that is already running as claimed", async () => {
    const id = seedTask({ sourceType: "manual", status: "running" });
    await expect(acquireTaskSlot(id)).resolves.toBe("claimed");
  });

  it("abandons a row that no longer exists", async () => {
    await expect(acquireTaskSlot(randomUUID())).resolves.toBe("abandoned");
  });

  it("abandons a completed row rather than re-running it", async () => {
    const id = seedTask({ sourceType: "manual", status: "completed" });
    await expect(acquireTaskSlot(id)).resolves.toBe("abandoned");
    expect(statusOf(id)).toBe("completed");
  });

  it("parks a resumable row as queued while it waits", async () => {
    vi.stubEnv("TASK_MAX_CONCURRENT", "1");
    const running = seedTask({ sourceType: "manual" });
    claimTaskSlot(running);

    const resumable = seedTask({ sourceType: "manual", status: "failed" });
    const waiter = acquireTaskSlot(resumable);
    await Promise.resolve();
    expect(statusOf(resumable)).toBe("queued");

    settle(running);
    expect(await waiter).toBe("claimed");
    expect(statusOf(resumable)).toBe("running");
  });

  it("drops a waiting run that was cancelled instead of starting it", async () => {
    vi.stubEnv("TASK_MAX_CONCURRENT", "1");
    const running = seedTask({ sourceType: "manual" });
    claimTaskSlot(running);

    const waiting = seedTask({ sourceType: "manual" });
    const waiter = acquireTaskSlot(waiting);
    await Promise.resolve();

    db.update(tasks)
      .set({ status: "cancelled" })
      .where(eq(tasks.id, waiting))
      .run();
    settle(running);

    expect(await waiter).toBe("abandoned");
    expect(statusOf(waiting)).toBe("cancelled");
  });

  it("never exceeds the cap when many runs are submitted at once", async () => {
    vi.stubEnv("TASK_MAX_CONCURRENT", "3");
    const ids = Array.from({ length: 12 }, () => seedTask({ sourceType: "manual" }));

    let peak = 0;
    const runs = ids.map(async (id) => {
      await acquireTaskSlot(id);
      peak = Math.max(peak, countRunningTaskSlots());
      settle(id);
    });

    await Promise.all(runs);
    expect(peak).toBeLessThanOrEqual(3);
    expect(countRunningTaskSlots()).toBe(0);
    expect(countWaitingTaskSlots()).toBe(0);
  });
});

describe("sweepOrphanedTaskRuns", () => {
  it("fails only on-demand runs left running by a previous process", () => {
    const orphan = seedTask({ sourceType: "manual", status: "running" });
    const untypedOrphan = seedTask({ sourceType: null, status: "running" });
    const scheduled = seedTask({ sourceType: "scheduled", status: "running" });
    const heartbeat = seedTask({ sourceType: "heartbeat", status: "running" });
    const queued = seedTask({ sourceType: "manual", status: "queued" });
    const done = seedTask({ sourceType: "manual", status: "completed" });

    const swept = sweepOrphanedTaskRuns().sort();

    expect(swept).toEqual([orphan, untypedOrphan].sort());
    expect(statusOf(orphan)).toBe("failed");
    expect(
      db.select().from(tasks).where(eq(tasks.id, orphan)).get()?.failureReason,
    ).toBe("process_restart");
    expect(statusOf(scheduled)).toBe("running");
    expect(statusOf(heartbeat)).toBe("running");
    expect(statusOf(queued)).toBe("queued");
    expect(statusOf(done)).toBe("completed");
  });

  it("keeps a result the runtime had already written", () => {
    const id = seedTask({
      sourceType: "manual",
      status: "running",
      result: "partial output",
    });

    sweepOrphanedTaskRuns();

    expect(
      db.select().from(tasks).where(eq(tasks.id, id)).get()?.result,
    ).toBe("partial output");
  });

  it("is idempotent", () => {
    seedTask({ sourceType: "manual", status: "running" });
    expect(sweepOrphanedTaskRuns()).toHaveLength(1);
    expect(sweepOrphanedTaskRuns()).toHaveLength(0);
  });
});
