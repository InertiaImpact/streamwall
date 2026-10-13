import type { StreamwallRuntimeDiagnostics } from 'streamwall-shared'

export interface RuntimeProcessMetricLike {
  pid: number
  type: string
  memory: {
    workingSetSize: number
  }
}

export interface RuntimeDiagnosticsInput {
  appVersion: string
  metrics: RuntimeProcessMetricLike[]
  gridRendererProcessIds: number[]
  activeViewCount: number
  systemMemory: {
    total: number
    free: number
  }
  now?: Date
}

const kibibytesToMegabytes = (value: number) =>
  Math.round((value / 1024) * 10) / 10

export function createRuntimeDiagnostics({
  appVersion,
  metrics,
  gridRendererProcessIds,
  activeViewCount,
  systemMemory,
  now = new Date(),
}: RuntimeDiagnosticsInput): StreamwallRuntimeDiagnostics {
  const rendererProcessIds = new Set(
    metrics
      .filter((metric) =>
        ['tab', 'renderer'].includes(metric.type.toLowerCase()),
      )
      .map((metric) => metric.pid),
  )
  const validGridRendererProcessIds = new Set(
    gridRendererProcessIds.filter((pid) => Number.isSafeInteger(pid) && pid > 0),
  )
  const totalWorkingSetKiB = metrics.reduce(
    (sum, metric) => sum + Math.max(0, metric.memory.workingSetSize),
    0,
  )
  const gpuWorkingSetKiB = metrics
    .filter((metric) => metric.type.toLowerCase() === 'gpu')
    .reduce(
      (sum, metric) => sum + Math.max(0, metric.memory.workingSetSize),
      0,
    )

  return {
    timestamp: now.toISOString(),
    appVersion,
    processCount: metrics.length,
    rendererProcessCount: rendererProcessIds.size,
    gridRendererProcessCount: validGridRendererProcessIds.size,
    activeViewCount,
    totalWorkingSetMB: kibibytesToMegabytes(totalWorkingSetKiB),
    gpuWorkingSetMB: kibibytesToMegabytes(gpuWorkingSetKiB),
    totalSystemMemoryMB: kibibytesToMegabytes(systemMemory.total),
    freeSystemMemoryMB: kibibytesToMegabytes(systemMemory.free),
  }
}
