import type { RalphConfig } from '../config/types.js';
import type { TrackerPlugin } from '../plugins/trackers/types.js';
import type { EngineEventListener } from '../engine/types.js';
import { analyzeTaskGraph, shouldRunParallel } from '../parallel/task-graph.js';
import { MergeEngine } from '../parallel/merge-engine.js';
import { ConflictResolver, type AiResolverCallback } from '../parallel/conflict-resolver.js';
import type { ParallelEvent, ParallelEventListener } from '../parallel/events.js';
import type { ParallelExecutorConfig, ParallelExecutorState, ParallelExecutorStatus, TaskGraphAnalysis, WorkerDisplayState, WorktreeInfo } from '../parallel/types.js';
import { DAGScheduler } from './scheduler.js';
import { WorkerRunner } from './runner.js';
import { ConflictCoordinator } from './conflicts.js';
import { startReady, waitNext, handleDone } from './engine.js';
import { saveTrackerState, restoreTrackerState, mergeProgressFile } from './utils.js';
import type { RunnerSlot } from './runner.js';

const DEFAULT: ParallelExecutorConfig = {
  maxWorkers: 3, worktreeDir: '.ralph-tui/worktrees', cwd: process.cwd(),
  maxIterationsPerWorker: 10, iterationDelay: 1000, aiConflictResolution: true, maxRequeueCount: 1,
};

export class ParallelExecutorDAG {
  cfg: ParallelExecutorConfig; base: RalphConfig; tracker: TrackerPlugin;
  runner: WorkerRunner; mergeEngine: MergeEngine; resolver: ConflictResolver; conflicts: ConflictCoordinator;
  status: ParallelExecutorStatus = 'idle'; taskGraph: TaskGraphAnalysis | null = null;
  scheduler: DAGScheduler | null = null; running = new Map<string, RunnerSlot>();
  completedCount = 0; failedCount = 0; mergeCount = 0; startedAt: string | null = null;
  shouldStop = false; paused = false; pauseWaiters: Array<() => void> = [];
  statusBeforePause: ParallelExecutorStatus | null = null;
  sessionId: string; requeueCounts = new Map<string, number>();
  returnToOriginalBranchError: string | null = null;
  preservedRecoveryWorktrees: WorktreeInfo[] = [];
  parallelListeners: ParallelEventListener[] = []; engineListeners: EngineEventListener[] = [];
  isResolving = false;

  constructor(base: RalphConfig, tracker: TrackerPlugin, opts?: Partial<ParallelExecutorConfig>) {
    this.base = base; this.tracker = tracker;
    this.sessionId = base.sessionId ?? `dag-${Date.now()}`;
    this.cfg = { ...DEFAULT, cwd: base.cwd, maxIterationsPerWorker: base.maxIterations, iterationDelay: base.iterationDelay, ...opts };
    this.runner = new WorkerRunner(this.cfg as any);
    this.mergeEngine = new MergeEngine(this.cfg.cwd);
    this.resolver = new ConflictResolver(this.cfg.cwd);
    const emit = (e: ParallelEvent) => this.emitParallel(e);
    this.conflicts = new ConflictCoordinator(this.mergeEngine, this.resolver, this.tracker, emit,
      () => saveTrackerState(this.tracker), s => restoreTrackerState(this.tracker, s), r => mergeProgressFile(this.cfg.cwd, r));
    this.mergeEngine.on(emit); this.resolver.on(emit);
  }

  on(l: ParallelEventListener): () => void { this.parallelListeners.push(l); return () => { const i = this.parallelListeners.indexOf(l); if (i >= 0) this.parallelListeners.splice(i, 1); }; }
  onEngineEvent(l: EngineEventListener): () => void { this.engineListeners.push(l); return () => { const i = this.engineListeners.indexOf(l); if (i >= 0) this.engineListeners.splice(i, 1); }; }
  setAiResolver(r: AiResolverCallback): void { this.resolver.setAiResolver(r); }

  reset(): void {
    this.shouldStop = false; this.status = 'idle'; this.taskGraph = null; this.scheduler = null; this.running.clear();
    this.completedCount = 0; this.failedCount = 0; this.mergeCount = 0; this.startedAt = null;
    this.requeueCounts.clear(); this.sessionId = `dag-${Date.now()}`; this.paused = false; this.pauseWaiters = [];
    this.statusBeforePause = null;
    this.returnToOriginalBranchError = null; this.preservedRecoveryWorktrees = [];
    this.isResolving = false;
  }

  async execute(): Promise<void> {
    this.startedAt = new Date().toISOString(); this.status = 'analyzing';
    try {
      let tasks = await this.tracker.getTasks({ status: ['open', 'in_progress'] });
      if (this.cfg.filteredTaskIds?.length) { const s = new Set(this.cfg.filteredTaskIds); tasks = tasks.filter(t => s.has(t.id)); }
      if (!tasks.length) { this.status = 'completed'; return; }
      this.taskGraph = analyzeTaskGraph(tasks); if (!shouldRunParallel(this.taskGraph)) { this.status = 'completed'; return; }
      this.scheduler = new DAGScheduler(this.taskGraph);
      if (!this.cfg.directMerge) {
        const { branch, original } = this.mergeEngine.initializeSessionBranch(this.sessionId, this.cfg.sessionBranchName);
        this.emitParallel({ type: 'parallel:session-branch-created', timestamp: new Date().toISOString(), sessionId: this.sessionId, sessionBranch: branch, originalBranch: original } as any);
      }
      this.mergeEngine.createSessionBackup(this.sessionId);
      this.emitParallel({ type: 'parallel:started', timestamp: this.startedAt, sessionId: this.sessionId, analysis: this.taskGraph, totalGroups: this.taskGraph.groups.length, totalTasks: this.taskGraph.actionableTaskCount, maxWorkers: this.cfg.maxWorkers } as any);
      this.status = 'executing';
      while (!this.shouldStop && (this.scheduler!.hasMoreWork() || this.running.size > 0 || this.conflicts.hasPending())) {
        await this.waitWhilePaused(); if (this.shouldStop) break;
        await startReady(this); const noRun = this.running.size === 0; const noWork = !this.scheduler!.hasMoreWork(); const pending = this.conflicts.hasPending();
        if (noRun && noWork && !pending) break;
        if (this.running.size) { const [id, r] = await waitNext(this); await handleDone(this, id, r); await startReady(this); }
        if (noRun && noWork && pending) await new Promise(r => setTimeout(r, 100));
      }
      const ok = !this.shouldStop && this.scheduler!.isAllCompleted(); this.status = ok ? 'completed' : 'interrupted';
      this.emitParallel({ type: 'parallel:completed', timestamp: new Date().toISOString(), sessionId: this.sessionId, totalTasksCompleted: this.completedCount, totalTasksFailed: this.failedCount, totalMergesCompleted: this.mergeCount, totalConflictsResolved: this.conflicts.getTotalResolved(), durationMs: this.startedAt ? Date.now() - new Date(this.startedAt).getTime() : 0 } as any);
    } catch (err) {
      this.status = 'failed'; const error = err instanceof Error ? err.message : String(err);
      this.emitParallel({ type: 'parallel:failed', timestamp: new Date().toISOString(), sessionId: this.sessionId, error, tasksCompletedBeforeFailure: this.completedCount } as any);
      throw err;
    } finally { await this.cleanup(); }
  }

  async retryConflictResolution(): Promise<boolean> {
    if (this.isResolving) return false;
    this.isResolving = true;
    try {
      const tid = this.conflicts.currentTaskId();
      const ok = await this.conflicts.retry();
      if (ok && tid) { this.completedCount++; this.failedCount--; this.mergeCount++; this.scheduler?.resolve(tid); await startReady(this); }
      return ok;
    } finally {
      this.isResolving = false;
    }
  }

  skipFailedConflict(): void {
    const tid = this.conflicts.skip();
    // Task was already moved to failed state and failedCount was incremented
    // when handleDone enqueued the conflict. We just need to skip it here.
    // Calling scheduler.fail() again is harmless (no-op on a failed task).
    if (tid) this.scheduler?.fail(tid);
  }

  hasPendingConflict(): boolean { return this.conflicts.hasPending(); }

  async stop(): Promise<void> { this.shouldStop = true; this.paused = false; this.releaseWaiters(); await Promise.allSettled(Array.from(this.running.values()).map(s => s.worker.stop())); this.status = 'interrupted'; }
  pause(): void {
    if (this.paused || this.status === 'completed' || this.status === 'failed') return;
    this.paused = true;
    this.statusBeforePause = this.status;
    this.status = 'paused';
    for (const s of this.running.values()) s.worker.pause();
  }
  resume(): void {
    if (!this.paused) return;
    this.paused = false;
    this.status = this.statusBeforePause ?? 'executing';
    this.statusBeforePause = null;
    this.releaseWaiters();
    for (const s of this.running.values()) s.worker.resume();
  }

  getState(): ParallelExecutorState {
    return { status: this.status, taskGraph: this.taskGraph, currentGroupIndex: this.completedCount, totalGroups: this.taskGraph?.groups.length ?? 0,
      workers: Array.from(this.running.values()).map(s => s.worker.getDisplayState()), mergeQueue: [...this.mergeEngine.getQueue()], completedMerges: [], activeConflicts: [],
      totalTasksCompleted: this.completedCount, totalTasks: this.taskGraph?.actionableTaskCount ?? 0, startedAt: this.startedAt, elapsedMs: this.startedAt ? Date.now() - new Date(this.startedAt).getTime() : 0 };
  }
  getSessionBranch(): string | null { return this.mergeEngine.getSessionBranch(); }
  getOriginalBranch(): string | null { return this.mergeEngine.getOriginalBranch(); }

  getWorkerStates(): WorkerDisplayState[] {
    return Array.from(this.running.values()).map(s => s.worker.getDisplayState());
  }

  getReturnToOriginalBranchError(): string | null {
    return this.returnToOriginalBranchError;
  }

  getPreservedRecoveryWorktrees(): WorktreeInfo[] {
    return [...this.preservedRecoveryWorktrees];
  }

  private async cleanup(): Promise<void> {
    const merged = new Set(this.mergeEngine.getQueue().filter(o => o.status === 'completed').map(o => o.sourceBranch)); const preserve = new Set<string>();
    for (const s of this.running.values()) if (s.branchName && !merged.has(s.branchName)) preserve.add(s.branchName);
    for (const o of this.mergeEngine.getQueue()) if (o.status !== 'completed' && o.sourceBranch) preserve.add(o.sourceBranch);
    // Capture preserved worktrees for recovery guidance (before cleanup removes them)
    this.preservedRecoveryWorktrees = this.runner.getAllWorktrees()
      .filter(info => preserve.has(info.branch))
      .map(info => ({ ...info }));
    await this.runner.cleanup({ preserveBranches: preserve }); try { this.mergeEngine.cleanupTags(); } catch {}
    if (!this.cfg.directMerge) {
      try { this.mergeEngine.returnToOriginalBranch(); this.returnToOriginalBranchError = null; }
      catch (err) { this.returnToOriginalBranchError = err instanceof Error ? err.message : String(err); }
    }
  }

  private async waitWhilePaused(): Promise<void> { while (this.paused && !this.shouldStop) await new Promise<void>(r => this.pauseWaiters.push(r)); }
  private releaseWaiters(): void { const w = this.pauseWaiters; this.pauseWaiters = []; for (const r of w) r(); }
  emitParallel(event: ParallelEvent): void { for (const l of this.parallelListeners) try { l(event); } catch {} }
}
