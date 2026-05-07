import type { TaskGraphAnalysis, TaskGraphNode } from '../parallel/types.js';
import type { TrackerTask } from '../plugins/trackers/types.js';

export class DAGScheduler {
  private nodes: Map<string, TaskGraphNode>;
  private ready = new Set<string>();
  private completed = new Set<string>();
  private failed = new Set<string>();
  private running = new Set<string>();

  constructor(analysis: TaskGraphAnalysis) {
    // Exclude cyclic tasks — they can never satisfy their dependencies
    // and must be handled (e.g., run sequentially) outside the DAG scheduler.
    // Without this exclusion, hasMoreWork() would return true forever and
    // the executor loop would spin.
    const cyclic = new Set(analysis.cyclicTaskIds);
    this.nodes = new Map(
      [...analysis.nodes].filter(([id]) => !cyclic.has(id)),
    );
    for (const [id, node] of this.nodes) {
      if (node.dependencies.length === 0) this.ready.add(id);
    }
  }

  nextReady(): TrackerTask | undefined {
    const candidates: TrackerTask[] = [];
    for (const id of this.ready) {
      if (this.running.has(id) || this.completed.has(id) || this.failed.has(id)) continue;
      const node = this.nodes.get(id);
      if (node) candidates.push(node.task);
    }
    candidates.sort((a, b) => a.priority - b.priority);
    return candidates[0];
  }

  start(id: string): void { this.running.add(id); }
  complete(id: string): void { this.running.delete(id); this.completed.add(id); this.unlock(id); }
  fail(id: string): void { this.running.delete(id); this.failed.add(id); }
  retry(id: string): void { this.failed.delete(id); if (this.isEligible(id)) this.ready.add(id); }

  resolve(id: string): void {
    if (!this.failed.has(id)) return;
    this.failed.delete(id);
    this.completed.add(id);
    this.unlock(id);
  }

  hasMoreWork(): boolean {
    for (const id of this.nodes.keys()) if (!this.completed.has(id) && !this.failed.has(id)) return true;
    return false;
  }

  isAllCompleted(): boolean {
    for (const id of this.nodes.keys()) if (!this.completed.has(id)) return false;
    return true;
  }

  private unlock(id: string): void {
    const node = this.nodes.get(id);
    if (!node) return;
    for (const dep of node.dependents) if (this.isEligible(dep)) this.ready.add(dep);
  }

  private isEligible(id: string): boolean {
    const node = this.nodes.get(id);
    return node ? node.dependencies.every(d => this.completed.has(d)) : false;
  }
}
