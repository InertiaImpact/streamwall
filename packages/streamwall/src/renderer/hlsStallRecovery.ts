export const STALL_RECOVERY_MS = 15_000
export const STALL_HARD_RELOAD_MS = 45_000
export const STALL_FAILURE_MS = 90_000
export const BUFFERED_RECOVERY_THRESHOLD_SECONDS = 2
export const BUFFERED_RECOVERY_NUDGE_SECONDS = 0.25

const RECOVERY_INTERVAL_MIN_MS = 12_500
const RECOVERY_INTERVAL_RANGE_MS = 5_000

export type StallRecoveryAction =
  | 'fail'
  | 'nudge-buffered-media'
  | 'restart-loading'
  | 'resume-loading'

export interface BufferedRangesLike {
  length: number
  start(index: number): number
  end(index: number): number
}

export function getBufferedAheadSeconds(
  ranges: BufferedRangesLike,
  currentTime: number,
) {
  for (let index = 0; index < ranges.length; index += 1) {
    const start = ranges.start(index)
    const end = ranges.end(index)
    if (currentTime >= start - 0.05 && currentTime < end) {
      return Math.max(0, end - currentTime)
    }
  }
  return 0
}

export function getJitteredRecoveryIntervalMs(random = Math.random) {
  const sample = Math.min(1, Math.max(0, random()))
  return Math.round(
    RECOVERY_INTERVAL_MIN_MS + sample * RECOVERY_INTERVAL_RANGE_MS,
  )
}

export function selectStallRecoveryAction({
  bufferedAheadSeconds,
  totalStallMs,
  hardReloadAttempted,
}: {
  bufferedAheadSeconds: number
  totalStallMs: number
  hardReloadAttempted: boolean
}): StallRecoveryAction {
  if (totalStallMs >= STALL_FAILURE_MS) return 'fail'
  if (bufferedAheadSeconds >= BUFFERED_RECOVERY_THRESHOLD_SECONDS) {
    return 'nudge-buffered-media'
  }
  if (totalStallMs >= STALL_HARD_RELOAD_MS && !hardReloadAttempted) {
    return 'restart-loading'
  }
  return 'resume-loading'
}
