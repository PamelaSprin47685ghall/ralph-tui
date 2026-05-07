import { WorktreeManager } from '../parallel/worktree-manager.js';
import { Worker } from '../parallel/worker.js';
import type { WorktreeInfo, WorkerResult } from '../parallel/types.js';
import type { TrackerTask } from '../plugins/trackers/types.js';
import type { RalphConfig } from '../config/types.js';
import type { EngineEventListener } from '../engine/types.js';
import type { ParallelEventListener } from '../parallel/events.js';

export interface RunnerSlot {
  worker: Worker;
  promise: Promise<WorkerResult>;
  branchName: string;
}

export class WorkerRunner {
  private worktreeManager: WorktreeManager;
  private index = 0;

  constructor(private cfg: { cwd: string; worktreeDir: string; maxWorkers: number; maxIterationsPerWorker: number }) {
    this.worktreeManager = new WorktreeManager({
      cwd: cfg.cwd,
      worktreeDir: cfg.worktreeDir,
      maxWorktrees: cfg.maxWorkers * 2,
    });
  }

  async start(
    task: TrackerTask,
    baseConfig: RalphConfig,
    tracker: any,
    onEvent: ParallelEventListener,
    onEngine: EngineEventListener
  ): Promise<RunnerSlot> {
    const id = `w-${this.index++}`;
    const info = await this.worktreeManager.acquire(id, task.id);
    const worker = new Worker(
      { id, task, worktreePath: info.path, branchName: info.branch, cwd: this.cfg.cwd },
      this.cfg.maxIterationsPerWorker
    );
    worker.on(onEvent);
    worker.onEngineEvent(onEngine);
    await worker.initialize(baseConfig, tracker);
    const promise = worker.start();
    return { worker, promise, branchName: info.branch };
  }

  release(workerId: string): void {
    this.worktreeManager.release(`worker-${workerId}`);
  }

  async cleanup(opts?: { preserveBranches?: ReadonlySet<string> }): Promise<WorktreeInfo[]> {
    const preserve = opts?.preserveBranches ?? new Set<string>();
    return this.worktreeManager.cleanupAll({ preserveBranches: preserve });
  }

  getAllWorktrees(): WorktreeInfo[] {
    return this.worktreeManager.getAllWorktrees();
  }
}
