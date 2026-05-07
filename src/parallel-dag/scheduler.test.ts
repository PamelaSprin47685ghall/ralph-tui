import { describe, test, expect } from 'bun:test';
import { DAGScheduler } from './scheduler.js';
import type { TaskGraphAnalysis, TaskGraphNode } from '../parallel/types.js';
import type { TrackerTask } from '../plugins/trackers/types.js';

function node(id: string, deps: string[] = [], depth = 0): [string, TaskGraphNode] {
  const t: TrackerTask = { id, title: id, description: '', status: 'open', priority: 2 };
  return [id, { task: t, dependencies: deps, dependents: [], depth, inCycle: false }];
}

function build(tasks: Array<{ id: string; deps: string[]; blocks?: string[] }>): TaskGraphAnalysis {
  const nodes = new Map(tasks.map(t => node(t.id, t.deps)));
  for (const t of tasks) {
    if (t.blocks) for (const b of t.blocks) {
      const n = nodes.get(b)!;
      n.dependencies.push(t.id);
      n.dependencies = [...new Set(n.dependencies)];
      const src = nodes.get(t.id)!;
      src.dependents.push(b);
      src.dependents = [...new Set(src.dependents)];
    }
  }
  return { nodes, groups: [], cyclicTaskIds: [], actionableTaskCount: tasks.length, maxParallelism: 0, recommendParallel: true };
}

describe('DAGScheduler', () => {
  test('starts with root tasks', () => {
    const s = new DAGScheduler(build([{ id: 'A', deps: [] }, { id: 'B', deps: [] }]));
    expect(s.nextReady()?.id).toBe('A');
    s.start('A');
    expect(s.nextReady()?.id).toBe('B');
  });

  test('unlocks dependents after complete', () => {
    const s = new DAGScheduler(build([{ id: 'A', deps: [] }, { id: 'B', deps: ['A'] }, { id: 'C', deps: [] }, { id: 'D', deps: ['B', 'C'] }]));
    expect(s.nextReady()?.id).toBeOneOf(['A', 'C']);
    s.complete('A');
    expect(s.nextReady()?.id).not.toBe('B');
  });

  test('retry adds back to ready', () => {
    const s = new DAGScheduler(build([{ id: 'A', deps: [] }]));
    s.start('A'); s.fail('A');
    expect(s.nextReady()?.id).toBeUndefined();
    s.retry('A');
    expect(s.nextReady()?.id).toBe('A');
  });
});
