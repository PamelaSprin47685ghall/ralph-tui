/**
 * ABOUTME: Event-driven orchestration for the DAG-based parallel executor.
 * Contains startReady, waitNext, and handleDone — the core loop functions
 * that implement true concurrency: as soon as a task completes, its dependents
 * are unlocked and can start immediately if worker slots are available.
 *
 * This replaces the old wave-based approach where all tasks in a group waited
 * for the slowest member before the next group could begin.
 */

import type { WorkerResult } from '../parallel/types.js';
import type { ParallelExecutorDAG } from './executor.js';

/**
 * Start as many ready tasks as possible, up to maxWorkers.
 * Pulls from the DAGScheduler's ready queue and starts workers via the Runner.
 */
export async function startReady(exec: ParallelExecutorDAG): Promise<void> {
  if (exec.shouldStop || exec.paused) return;

  while (exec.running.size < exec.cfg.maxWorkers) {
    if (exec.shouldStop || exec.paused) break;

    const task = exec.scheduler!.nextReady();
    if (!task) break;

    exec.scheduler!.start(task.id);

    // Forward worker events to executor's parallel listeners
    const onEvent = (e: any): void => exec.emitParallel(e);
    // Forward engine events to executor's engine listeners
    const onEngine = (e: any): void => {
      for (const l of exec.engineListeners) {
        try { l(e); } catch { /* swallow */ }
      }
    };

    try {
      const slot = await exec.runner.start(task, exec.base, exec.tracker, onEvent, onEngine);
      exec.running.set(task.id, slot);

      // Mark task as in_progress in the tracker (best effort)
      try { await exec.tracker.updateTaskStatus(task.id, 'in_progress'); } catch { /* non-fatal */ }

      exec.emitParallel({
        type: 'worker:started',
        timestamp: new Date().toISOString(),
        workerId: slot.worker.id,
        task,
      } as any);
    } catch (err) {
      // Worker creation failed — fail the task in scheduler so we don't retry forever
      exec.scheduler!.fail(task.id);
      exec.failedCount++;
      try { await exec.tracker.updateTaskStatus(task.id, 'open'); } catch { /* non-fatal */ }
    }
  }
}

/**
 * Wait for any running worker to complete and return its task ID and result.
 * Uses Promise.race on all in-flight worker promises.
 *
 * IMPORTANT: Caller MUST ensure exec.running is non-empty before calling.
 * Promise.race([]) returns a promise that never settles.
 */
export async function waitNext(exec: ParallelExecutorDAG): Promise<[string, WorkerResult]> {
  const entries = Array.from(exec.running.entries());
  if (entries.length === 0) {
    throw new Error('waitNext called with empty running map — no workers to wait for');
  }
  // Race all running workers — the first to finish wins
  const winner = await Promise.race(
    entries.map(([id, slot]) =>
      slot.promise.then((result) => [id, result] as [string, WorkerResult])
    ),
  );
  return winner;
}

/**
 * Process a completed worker: remove from running map, merge its branch,
 * handle conflicts, and update scheduler state.
 *
 * On merge success → scheduler.complete(id)
 * On merge conflict → enqueue to ConflictCoordinator for later AI/user resolution
 * On merge failure → scheduler.fail(id) with retry budget check
 * On worker failure → scheduler.fail(id)
 */
export async function handleDone(
  exec: ParallelExecutorDAG,
  id: string,
  result: WorkerResult,
): Promise<void> {
  // Capture worker ID before removing from running map
  const slot = exec.running.get(id);
  const workerId = slot?.worker.id;

  // Remove from running map
  exec.running.delete(id);

  // Release the worktree slot so it can be reused by another task
  if (workerId) {
    try { exec.runner.release(workerId); } catch { /* non-fatal */ }
  }

  if (result.success && result.taskCompleted) {
    // Save tracker state before merging (prevent stale .beads overwrite)
    const savedState = await saveTrackerState(exec.tracker);
    let mergeResult: Awaited<ReturnType<typeof exec.mergeEngine.processNext>> = null;

    try {
      exec.mergeEngine.enqueue(result);
      // processNext only processes one operation; it's sequential per-call
      mergeResult = await exec.mergeEngine.processNext();
    } finally {
      await restoreTrackerState(exec.tracker, savedState);
    }

    if (mergeResult?.success) {
      // Merge succeeded
      try { await exec.tracker.completeTask(id); } catch { /* non-fatal */ }
      await mergeProgressFile(exec.cfg.cwd, result);
      exec.requeueCounts.delete(id);
      exec.scheduler!.complete(id);
      exec.completedCount++;
      exec.mergeCount++;
    } else if (mergeResult?.hadConflicts) {
      // Merge conflicts — find the operation and enqueue for resolution
      const operation = exec.mergeEngine
        .getQueue()
        .find((op) => op.id === mergeResult.operationId);

      if (operation && exec.cfg.aiConflictResolution) {
        exec.conflicts.enqueue(operation, result);
        // Move task from running to failed so scheduler knows it's blocked.
        // failedCount tracks the blocked task; retryConflictResolution will
        // decrement it on success (via resolve → completed) and increment
        // mergeCount. This also prevents hasMoreWork() from seeing a stuck
        // "running" task that will never complete on its own, which avoids
        // the hot spin loop.
        exec.scheduler!.fail(id);
        exec.failedCount++;
      } else {
        // AI resolution disabled or operation not found — fall back to retry/fail
        await handleMergeFailure(exec, result);
      }
    } else {
      // Non-conflict merge failure — retry or fail
      await handleMergeFailure(exec, result);
    }
  } else {
    // Worker failed or task not marked complete
    exec.scheduler!.fail(id);
    exec.failedCount++;
    try { await exec.tracker.updateTaskStatus(id, 'open'); } catch { /* non-fatal */ }
  }
}

/**
 * Handle a merge failure (conflict resolution exhausted or non-conflict error).
 * Checks retry budget and either re-queues the task or marks it permanently failed.
 */
async function handleMergeFailure(
  exec: ParallelExecutorDAG,
  result: WorkerResult,
): Promise<void> {
  const id = result.task.id;
  const currentCount = exec.requeueCounts.get(id) ?? 0;

  if (currentCount < exec.cfg.maxRequeueCount) {
    // Has retry budget — move task back to ready
    // Must fail() first to take it out of running state, then retry() to
    // move from failed → ready. Without fail(), the task stays in running
    // and nextReady() skips it even though it's also in the ready set.
    exec.requeueCounts.set(id, currentCount + 1);
    exec.scheduler!.fail(id);
    exec.scheduler!.retry(id);
    try { await exec.tracker.updateTaskStatus(id, 'open'); } catch { /* non-fatal */ }
  } else {
    // Retry budget exhausted — permanently fail
    exec.scheduler!.fail(id);
    exec.failedCount++;
    try { await exec.tracker.updateTaskStatus(id, 'open'); } catch { /* non-fatal */ }
  }
}

// Re-use the utils functions from the existing module
import { saveTrackerState, restoreTrackerState, mergeProgressFile } from './utils.js';
