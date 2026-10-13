import assert from 'node:assert/strict'
import test from 'node:test'
import { createRuntimeDiagnostics } from './runtimeDiagnostics'

test('summarizes process, renderer, and memory diagnostics', () => {
  const diagnostics = createRuntimeDiagnostics({
    appVersion: '2.2.7-bs',
    metrics: [
      { pid: 1, type: 'Browser', memory: { workingSetSize: 1024 } },
      { pid: 2, type: 'Tab', memory: { workingSetSize: 2048 } },
      { pid: 3, type: 'Tab', memory: { workingSetSize: 3072 } },
      { pid: 4, type: 'GPU', memory: { workingSetSize: 4096 } },
    ],
    gridRendererProcessIds: [2, 2, 3, 0],
    activeViewCount: 81,
    systemMemory: { total: 64 * 1024, free: 16 * 1024 },
    now: new Date('2026-08-28T20:00:00.000Z'),
  })

  assert.deepEqual(diagnostics, {
    timestamp: '2026-08-28T20:00:00.000Z',
    appVersion: '2.2.7-bs',
    processCount: 4,
    rendererProcessCount: 2,
    gridRendererProcessCount: 2,
    activeViewCount: 81,
    totalWorkingSetMB: 10,
    gpuWorkingSetMB: 4,
    totalSystemMemoryMB: 64,
    freeSystemMemoryMB: 16,
  })
})
