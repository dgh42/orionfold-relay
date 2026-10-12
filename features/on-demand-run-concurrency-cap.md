---
title: On-Demand Run Concurrency Cap
status: completed
shipped-date: 2026-10-12
priority: P1
milestone: post-mvp
source: uncapped manual, chat, handoff and workflow-step starts
dependencies: [schedule-collision-prevention, agent-integration, workflow-engine]
---

# On-Demand Run Concurrency Cap

## Description

`schedule-collision-prevention` capped schedule firings at
`SCHEDULE_MAX_CONCURRENT` and made the claim atomic. The cap matches only
`source_type IN ('scheduled', 'heartbeat')`, so it governed the unattended half
of task execution and left the attended half open.

Everything else — the manual execute route, resume, the chat `execute_task`
tool, the handoff bus, and every workflow step — reached `startTaskExecution`
and was marked `running` unconditionally. A workflow that fans out to parallel
branches, or an operator clicking Run on a dozen tasks, started as many agent
processes as there was work for. On a small host that exhausts memory and every
run in flight fails together.

This feature gives those runs a cap of their own.

## Behaviour

**A second, independent cap.** `TASK_MAX_CONCURRENT`, then the
`task.maxConcurrent` setting, then **20**. It governs every task run whose
`source_type` is not `scheduled` or `heartbeat`, `NULL` included. The two counts
are exact complements, so a full schedule cap cannot block an on-demand run and
a full on-demand cap cannot block a firing.

**The claim is atomic.** One `UPDATE` with the count as a subquery in its
`WHERE` clause, the same shape as `SLOT_CLAIM_SQL`. Two concurrent start paths
cannot both take the last slot.

**Over the cap, nothing fails.** The row stays `queued` and the run takes a
place in a FIFO line, in submission order. It starts when a slot frees — drained
when a run settles, and by a timer that exists only while something is waiting.
A caller that awaits the run, including a workflow step, waits longer rather
than erroring.

**The routes say so.** `POST /api/tasks/:id/execute` and
`POST /api/tasks/:id/resume` answer `202` either way, with `state: "running"` or
`state: "queued"` and the cap that applied. `?force=true` on execute bypasses the
cap and writes a `manual_force_bypass` row to `usage_ledger` before dispatch.

**Orphans are swept at startup, not reaped on a timer.** On-demand runs carry no
lease: an agent run has no bounded duration to measure against, so a reaper could
only ever kill a long legitimate run. A row still `running` when a new process
boots is unambiguously dead, and `sweepOrphanedTaskRuns()` fails exactly those
with `failure_reason = 'process_restart'` before anything can start.

**`source_type` is explicit.** Workflow step tasks are stamped `workflow`; tasks
created through the API and the chat `create_task` tool are stamped `manual`. A
`NULL` still counts as on-demand, so rows that already exist are unaffected.

## Design decisions

The full record, including the deadlock analysis of every awaiting start path and
why the waiting line is held in memory rather than in the `tasks` table, is
`.agents/skills/architect/references/tdr-on-demand-run-cap.md`.

## Implementation

| Area | File |
| --- | --- |
| Cap, claim, FIFO line, drain, startup sweep | `src/lib/agents/task-slots.ts` |
| The gate every on-demand run passes | `src/lib/agents/task-dispatch.ts` |
| Settings key | `src/lib/constants/settings.ts` |
| Shared config reader | `src/lib/schedules/config.ts` |
| Routes | `src/app/api/tasks/[id]/execute/route.ts`, `src/app/api/tasks/[id]/resume/route.ts` |
| Workflow step provenance and claim | `src/lib/workflows/engine.ts` |
| Startup sweep wiring | `src/instrumentation-node.ts` |

## Tests

`node` project:

- `src/lib/agents/__tests__/task-slots.test.ts` — the cap and its default,
  config precedence, the claim refusing over the cap and leaving the row queued,
  two real SQLite connections racing beneath cap 1, the two caps' independence,
  FIFO ordering across three deferred runs, a cancelled waiter dropped rather
  than started, 12 runs submitted at once never exceeding a cap of 3, `force`,
  and the startup sweep touching only orphaned on-demand runs.
- `src/lib/agents/__tests__/task-dispatch.test.ts` — the dispatcher gate holds an
  on-demand run and starts it when a slot frees, and does not gate a firing.
- `src/lib/workflows/__tests__/child-task-concurrency.test.ts` — three parallel
  branches complete with the cap set to 1, never more than one at a time.
- `src/app/api/tasks/[id]/execute/__tests__/route.test.ts` and
  `.../resume/__tests__/route.test.ts` — the `202 queued` answer and the audited
  `force` bypass.
