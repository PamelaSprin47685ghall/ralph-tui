import { readFile, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import type { TrackerPlugin } from '../plugins/trackers/types.js';
import type { WorkerResult } from '../parallel/types.js';

export async function saveTrackerState(tracker: TrackerPlugin): Promise<Map<string, string>> {
  const saved = new Map<string, string>();
  const getStateFiles = (tracker as any).getStateFiles;
  if (typeof getStateFiles !== 'function') return saved;
  for (const fp of getStateFiles()) {
    try { saved.set(fp, await readFile(fp, 'utf-8')); } catch {}
  }
  return saved;
}

export async function restoreTrackerState(tracker: TrackerPlugin, saved: Map<string, string>): Promise<void> {
  for (const [fp, content] of saved) {
    try {
      await writeFile(fp, content, 'utf-8');
      const clearCache = (tracker as any).clearCache;
      if (typeof clearCache === 'function') clearCache();
    } catch {}
  }
}

export async function mergeProgressFile(cwd: string, result: WorkerResult): Promise<void> {
  if (!result.worktreePath) return;
  const workerPath = join(result.worktreePath, '.ralph-tui', 'progress.md');
  const mainPath = join(cwd, '.ralph-tui', 'progress.md');
  try {
    const { access, constants, readFile, appendFile } = await import('node:fs/promises');
    await access(workerPath, constants.R_OK);
    const content = await readFile(workerPath, 'utf-8');
    if (!content.trim()) return;
    const sep = `\n\n---\n\n## Parallel Task: ${result.task.title} (${result.task.id})\n\n`;
    await appendFile(mainPath, sep + content);
  } catch {}
}
