import type { HlsConfig, LoaderResponse, RetryConfig } from 'hls.js'

const TRANSIENT_HTTP_STATUSES = new Set([404, 408, 425, 429])

export const TRAFFIC_CAMERA_STARTUP_FAILURE_MS = 90_000
export const TRAFFIC_CAMERA_STARTUP_GRACE_MS = 5_000
export const TRAFFIC_CAMERA_REQUEST_ADMISSION_FAILURE_MS = 180_000
const HAVE_CURRENT_DATA = 2

export function shouldGrantStartupGrace(
  playbackStarted: boolean,
  readyState: number,
) {
  return !playbackStarted && readyState >= HAVE_CURRENT_DATA
}

export function shouldMarkPlaybackProgress(
  reportedFatal: boolean,
  readyState: number,
) {
  return !reportedFatal && readyState >= HAVE_CURRENT_DATA
}

export interface TrafficCameraHlsConfigOptions {
  random?: () => number
}

function shouldRetryTransientResponse(
  response: LoaderResponse | undefined,
  defaultDecision: boolean,
  retryNotFound: boolean,
) {
  // A missing live media segment has already fallen out of the origin's
  // sliding window. Retrying that URL cannot recover it; the player must
  // refresh the playlist and resume at the live edge instead.
  if (response?.code === 404 && !retryNotFound) return false
  return (
    defaultDecision ||
    (response?.code != null && TRANSIENT_HTTP_STATUSES.has(response.code))
  )
}

function createRetryConfig(
  maxNumRetry: number,
  baseDelayMs: number,
  maxRetryDelayMs: number,
  random: () => number,
  retryNotFound = true,
): RetryConfig {
  const applyJitter = (retryConfig: RetryConfig | null | undefined) => {
    if (!retryConfig) return
    // Recalculate before every retry so players do not repeatedly converge on
    // the same segment boundary after a shared origin failure.
    const jitterFactor = 0.75 + random() * 0.5
    retryConfig.retryDelayMs = Math.round(baseDelayMs * jitterFactor)
  }

  return {
    maxNumRetry,
    retryDelayMs: baseDelayMs,
    maxRetryDelayMs,
    backoff: 'exponential',
    shouldRetry: (
      retryConfig,
      _retryCount,
      _isTimeout,
      response,
      defaultDecision,
    ) => {
      applyJitter(retryConfig)
      return shouldRetryTransientResponse(
        response,
        defaultDecision,
        retryNotFound,
      )
    },
  }
}

export function createTrafficCameraHlsConfig(
  options: TrafficCameraHlsConfigOptions = {},
): Partial<HlsConfig> {
  const random = options.random ?? Math.random

  return {
    // These walls favor continuity over sub-second latency. The measured Iowa
    // DOT origins routinely need several seconds to return playlists/segments.
    lowLatencyMode: false,
    // Each camera already runs in an isolated Electron renderer. Creating an
    // additional transmuxing worker per tile multiplies process/thread overhead
    // without isolating work any further.
    enableWorker: false,
    startLevel: -1,
    defaultAudioCodec: undefined,
    maxBufferLength: 20,
    maxMaxBufferLength: 30,
    // Retain the forward continuity buffer, but do not keep a second large
    // history buffer for dozens of live-only tiles that never seek backwards.
    backBufferLength: 2,
    maxBufferSize: 12 * 1024 * 1024,
    // Iowa DOT's KVDS playlists currently retain only seven complete
    // segments. Staying two segments behind preserves a small continuity
    // buffer without selecting media that is already close to eviction.
    liveSyncDurationCount: 2,
    liveMaxLatencyDurationCount: 3,
    maxLiveSyncPlaybackRate: 1.05,
    capLevelToPlayerSize: true,
    liveDurationInfinity: true,
    manifestLoadPolicy: {
      default: {
        maxTimeToFirstByteMs: 20_000,
        maxLoadTimeMs: 30_000,
        timeoutRetry: createRetryConfig(2, 1_000, 10_000, random),
        errorRetry: createRetryConfig(3, 1_000, 10_000, random),
      },
    },
    playlistLoadPolicy: {
      default: {
        maxTimeToFirstByteMs: 20_000,
        maxLoadTimeMs: 30_000,
        timeoutRetry: createRetryConfig(3, 750, 10_000, random),
        errorRetry: createRetryConfig(4, 750, 10_000, random),
      },
    },
    fragLoadPolicy: {
      default: {
        maxTimeToFirstByteMs: 20_000,
        maxLoadTimeMs: 60_000,
        timeoutRetry: createRetryConfig(4, 750, 12_000, random),
        errorRetry: createRetryConfig(6, 750, 12_000, random, false),
      },
    },
  }
}

export interface HlsLoadErrorLike {
  details?: unknown
  responseCode?: unknown
  url?: unknown
}

export function isExpiredMediaSegmentError(error: HlsLoadErrorLike) {
  return (
    error.details === 'fragLoadError' &&
    error.responseCode === 404
  )
}
