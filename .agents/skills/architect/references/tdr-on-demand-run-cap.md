# TDR: On-demand runs have their own cap, their own queue, and no lease

**Status:** Accepted
**Date:** 2026-10-12

## Context

`SCHEDULE_MAX_CONCURRENT` caps schedule firings, and `claimSlot` enforces it in
one atomic statement (`tdr-atomic-slot-claim.md`). That statement matches only
`source_type IN ('scheduled', 'heartbeat')`. Every other way a task run starts —
the manual execute route, resume, the chat `execute_task` tool, the handoff bus,
a workflow step — reached `startTaskExecution` and was marked `running`
unconditionally.

So the cap covered the unattended path and left the attended one open. A
workflow that fans out to parallel branches, or a burst of manual executes,
launched as many agent processes as it had work for. On a small host that
exhausts memory and every run in flight fails together.

A second gap: a run left `running` when the process stopped stayed `running`
forever. Only scheduled runs carry a lease for `reapExpiredLeases` to collect.

## Decision

A second, independent cap for **on-demand** runs — every task run whose
`source_type` is not `scheduled` or `heartbeat`, `NULL` included. Default 20,
read as `TASK_MAX_CONCURRENT`, then the `task.maxConcurrent` setting, then the
default, through the same `readIntConfig` the schedule cap uses.

Five decisions follow from it.

**The two caps stay independent and disjoint.** The on-demand count is the exact
complement of the scheduled one, so every running task is counted once and
neither cap can be filled by the other's work. The schedule cap is low (2)
because a firing is unattended and arrives in bursts; an on-demand run is
something a person or a workflow is waiting on, so its cap is set high enough to
be invisible in normal use and only bites when a burst would otherwise exhaust
the host. `tdr-evidence-based-cap.md` governs the schedule number and does not
govern this one.

**The claim is one SQL statement**, with the count as a subquery in the `WHERE`
clause, for the reason `tdr-atomic-slot-claim.md` gives: every start path can run
concurrently, so check-then-act would let two callers both read "one slot left".
It writes `slot_claimed_at` and deliberately writes no lease.

**The waiting line is in memory, not in the table.** This is the decision most
worth recording, because the obvious alternative is wrong here. `drainQueue`
walks the `tasks` table for queued schedule firings, which is safe because a
scheduled task row only exists once it has fired. For on-demand work `queued` is
a *resting* state — a task a person created and has not run yet sits in it
indefinitely, and the execute route refuses anything else — so a drain that
walked every queued row would launch runs nobody asked for. The in-memory list
holds only runs that were actually submitted and that the cap deferred.

The cost is that a restart empties the line. That leaves those rows `queued`,
which is where they would have been had the process died a moment earlier, and a
person can run them again. Nothing is failed, and nothing is started silently.

FIFO is therefore by **submission order**, not by `created_at`. For a run that
was submitted the moment it was created the two agree; where they disagree,
submission order is the one that means "first come, first served".

**Over the cap is never an error.** The row stays `queued`, the route answers
`202` with `state: "queued"`, and the dispatcher holds the caller until its turn
comes. A caller that awaits the run — `executeChildTask` above all — simply waits
longer. `?force=true` on the manual execute route bypasses the cap and writes a
`manual_force_bypass` row to `usage_ledger` before dispatch, as
`tdr-manual-honors-cap.md` prescribes for every "run now" surface.

**Orphans are swept once at startup, and never reaped on a timer.** A periodic
reaper needs a duration to measure against, and an on-demand run has none: the
only thing a timer could reliably kill is a long legitimate run. A row still
`running` after a restart, by contrast, is unambiguously dead — nothing in the
new process is executing it. `sweepOrphanedTaskRuns()` fails exactly those, with
`failure_reason = 'process_restart'`, before anything can start a new run.

## Deadlock analysis

A capped run that waits on another capped run deadlocks if the waiter holds a
slot its dependency needs. Every path that can start a run was read for this:

| Path | Awaits the run? | Does the waiter hold a slot? |
| --- | --- | --- |
| `executeChildTask` (workflow step, every pattern incl. parallel and swarm) | yes | **no** — the workflow engine is driven by `executeWorkflow`, which is fire-and-forget from its routes, the chat tool, the app-trigger dispatcher and the scheduler. A workflow is not a task row and holds no slot. |
| Manual execute route | no | n/a |
| Resume route | no | n/a |
| Chat `execute_task` | no — fire-and-forget | n/a (and a chat conversation is not a task row) |
| Handoff bus | no — fire-and-forget, dispatched from the scheduler tick | n/a |
| Heartbeat evaluation | yes, but `source_type = 'heartbeat'` — not capped here | n/a |

So no cycle exists today: nothing that holds an on-demand slot awaits another
on-demand run. A workflow's parallel branches are siblings, not a parent and a
child, so with the cap at 1 they run one at a time and the fan-out completes.
`src/lib/workflows/__tests__/child-task-concurrency.test.ts` pins that.

The invariant to keep is therefore: **a running task must not await another task
run.** If a future feature needs one — a step that synchronously delegates to a
sub-task, an awaiting handoff — the fix is not a bigger cap but releasing the
parent's slot for the duration of the wait, because any cap can be filled by
enough nested waiters.

## Consequences

- `source_type` is now written on workflow step tasks (`workflow`) and on tasks
  created through the API and the chat tool (`manual`), so the count is explicit
  rather than inferred from `NULL`. `NULL` still counts as on-demand, which is
  what makes the change safe for rows that already exist.
- `executeChildTask` no longer sets its step task `running` before dispatch; the
  claim makes that transition, and setting it first would have made the step
  count against the cap before it had a slot.
- No new table and no new index: `idx_tasks_running_scheduled` is
  `(status, source_type, lease_expires_at)`, and its first two columns are
  exactly what the on-demand count filters on.
- A missed release can delay a queued run until the next drain tick; it can
  never let the cap be exceeded, because the cap is counted from the table on
  every claim rather than from an in-memory number.
