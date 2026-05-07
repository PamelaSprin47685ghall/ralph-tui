import type { MergeEngine } from '../parallel/merge-engine.js';
import type { ConflictResolver } from '../parallel/conflict-resolver.js';
import type { TrackerPlugin } from '../plugins/trackers/types.js';
import type { MergeOperation, WorkerResult } from '../parallel/types.js';
import type { ParallelEvent } from '../parallel/events.js';

export class ConflictCoordinator {
  private pending: Array<{ operation: MergeOperation; workerResult: WorkerResult }> = [];
  private totalResolved = 0;

  constructor(
    private mergeEngine: MergeEngine,
    private resolver: ConflictResolver,
    private tracker: TrackerPlugin,
    private emit: (e: ParallelEvent) => void,
    private saveState: () => Promise<Map<string, string>>,
    private restoreState: (s: Map<string, string>) => Promise<void>,
    private mergeProgress: (r: WorkerResult) => Promise<void>
  ) {}

  enqueue(operation: MergeOperation, workerResult: WorkerResult): void {
    if (this.pending.some(p => p.operation.id === operation.id)) return;
    this.pending.push({ operation, workerResult });
    this.emitPending();
  }

  hasPending(): boolean {
    return this.pending.length > 0;
  }

  currentTaskId(): string | undefined {
    return this.pending[0]?.workerResult.task.id;
  }

  async retry(): Promise<boolean> {
    const entry = this.pending[0];
    if (!entry) return false;
    const saved = await this.saveState();
    try {
      const resolutions = await this.resolver.resolveConflicts(entry.operation);
      if (resolutions.every(r => r.success)) {
        this.pending.shift();
        try { await this.tracker.completeTask(entry.workerResult.task.id); } catch {}
        await this.mergeProgress(entry.workerResult);
        this.totalResolved += resolutions.length;
        this.emitPending();
        return true;
      }
      return false;
    } finally {
      await this.restoreState(saved);
    }
  }

  skip(): string | undefined {
    const entry = this.pending.shift();
    if (!entry) return undefined;
    this.mergeEngine.markOperationRolledBack(entry.operation.id, 'Skipped by user');
    this.emit({
      type: 'conflict:resolved',
      timestamp: new Date().toISOString(),
      operationId: entry.operation.id,
      taskId: entry.workerResult.task.id,
      results: [],
    } as any);
    this.emitPending();
    return entry.workerResult.task.id;
  }

  getTotalResolved(): number {
    return this.totalResolved;
  }

  private emitPending(): void {
    const next = this.pending[0];
    if (!next) return;
    this.emit({
      type: 'conflict:detected',
      timestamp: new Date().toISOString(),
      operationId: next.operation.id,
      taskId: next.workerResult.task.id,
      conflicts: (next.operation.conflictedFiles ?? []).map(f => ({
        filePath: f,
        oursContent: '',
        theirsContent: '',
        baseContent: '',
        conflictMarkers: '',
      })),
    } as any);
  }
}
