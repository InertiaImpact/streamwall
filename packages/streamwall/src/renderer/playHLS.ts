import Hls from 'hls.js'
import { CropRect } from 'streamwall-shared'
import { CoalescingReporter } from './coalescingReporter'
import {
  createTrafficCameraHlsConfig,
  isExpiredMediaSegmentError,
  shouldGrantStartupGrace,
  shouldMarkPlaybackProgress,
  TRAFFIC_CAMERA_REQUEST_ADMISSION_FAILURE_MS,
  TRAFFIC_CAMERA_STARTUP_FAILURE_MS,
  TRAFFIC_CAMERA_STARTUP_GRACE_MS,
} from './hlsTrafficCameraConfig'
import {
  BUFFERED_RECOVERY_NUDGE_SECONDS,
  getBufferedAheadSeconds,
  getJitteredRecoveryIntervalMs,
  selectStallRecoveryAction,
  STALL_RECOVERY_MS,
} from './hlsStallRecovery'
import { ScheduledHlsLoader } from './scheduledHlsLoader'

// Parse src immediately on load
const searchParams = new URLSearchParams(location.search)
const src = searchParams.get('src')
const cropParam = searchParams.get('crop')
const startupDetail = Object.fromEntries(
  ['activeStartups', 'queuedStartups', 'queueWaitMs'].flatMap((key) => {
    const value = Number(searchParams.get(key))
    return Number.isFinite(value) && value >= 0 ? [[key, value]] : []
  }),
)

let crop: CropRect | undefined
if (cropParam) {
  const tryParsers = [
    () => JSON.parse(cropParam),
    () => JSON.parse(atob(cropParam)),
  ]
  for (const parse of tryParsers) {
    try {
      crop = parse()
      break
    } catch (err) {
      // try next parser
    }
  }
  if (!crop) {
    console.warn('Invalid crop param', cropParam)
  }
}

console.debug('PlayHLS loading with src:', src)
if (crop) {
  console.debug('PlayHLS crop:', crop)
}

// Ensure the page never shows scrollbars even when the video is overscaled for cropping.
document.documentElement.style.overflow = 'hidden'
document.documentElement.style.overscrollBehavior = 'none'
document.body.style.overflow = 'hidden'
document.body.style.overscrollBehavior = 'none'

// Create and append video immediately
const videoEl = document.createElement('video')
videoEl.autoplay = true
videoEl.muted = true
videoEl.playsInline = true
videoEl.style.position = 'absolute'
videoEl.style.top = '0'
videoEl.style.left = '0'
videoEl.style.width = '100%'
videoEl.style.height = '100%'
videoEl.style.objectFit = 'fill'
videoEl.style.display = 'block'
videoEl.style.margin = '0'
videoEl.style.padding = '0'
videoEl.loop = false // Explicitly disable looping for live streams
videoEl.controls = false
document.body.appendChild(videoEl)

// Clear any stale sizing/transform immediately (in case this WebContents is reused)
applyCrop()

function applyCrop() {
  // If no crop is provided, ensure default sizing (no translation/scaling) and preserve aspect.
  if (!crop) {
    videoEl.style.width = '100%'
    videoEl.style.height = '100%'
    videoEl.style.transformOrigin = ''
    videoEl.style.transform = ''
    videoEl.style.objectFit = 'contain'
    return
  }

  if (!videoEl.videoWidth || !videoEl.videoHeight) {
    return
  }

  const containerWidth = window.innerWidth
  const containerHeight = window.innerHeight
  if (!containerWidth || !containerHeight) {
    return
  }

  const scale = Math.max(
    containerWidth / crop.width,
    containerHeight / crop.height,
  )
  const targetWidth = videoEl.videoWidth * scale
  const targetHeight = videoEl.videoHeight * scale
  videoEl.style.width = `${targetWidth}px`
  videoEl.style.height = `${targetHeight}px`
  videoEl.style.transformOrigin = 'top left'
  videoEl.style.transform = `translate(${-crop.x * scale}px, ${-crop.y * scale}px)`
  videoEl.style.objectFit = 'fill'
}

// Add event listeners to detect and prevent looping
videoEl.addEventListener('ended', (e) => {
  console.debug('Video ended event - pausing playback')
  videoEl.pause()
  videoEl.currentTime = 0
})

// Track progress to detect stalls and auto-recover
let lastProgressTs = performance.now()
let lastTime = 0
let fatalErrorCount = 0
videoEl.addEventListener('loadedmetadata', applyCrop)
window.addEventListener('resize', applyCrop)

// Start HLS loading immediately if src exists
if (src) {
  const MAX_FATAL_ERRORS = 3
  const NONFATAL_LOG_WINDOW_MS = 10_000

  let startupTimer: number | undefined
  let admissionTimer: number | undefined
  let reportedFatal = false
  let playbackStarted = false
  let stallStartedAt: number | undefined
  let nextRecoveryAttemptAt = 0
  let hardReloadAttempted = false
  let lastExpiredSegmentResyncAt = 0

  const reportDebug = (
    level: 'info' | 'warning' | 'error',
    event: string,
    detail?: unknown,
  ) => {
    window.postMessage(
      { type: 'streamwall:hls-log', level, event, detail, src },
      '*',
    )
  }

  const hls = new Hls({
    ...createTrafficCameraHlsConfig(),
    loader: ScheduledHlsLoader,
  })

  const markNetworkSuccess = () => {
    fatalErrorCount = 0
  }

  const markPlaybackProgress = () => {
    if (!shouldMarkPlaybackProgress(reportedFatal, videoEl.readyState)) return

    const firstPlaybackProgress = !playbackStarted
    lastProgressTs = performance.now()
    lastTime = videoEl.currentTime
    fatalErrorCount = 0
    playbackStarted = true
    stallStartedAt = undefined
    nextRecoveryAttemptAt = 0
    hardReloadAttempted = false
    if (startupTimer) {
      window.clearTimeout(startupTimer)
      startupTimer = undefined
    }
    if (admissionTimer) {
      window.clearTimeout(admissionTimer)
      admissionTimer = undefined
    }
    if (firstPlaybackProgress) {
      reportDebug('info', 'playback-started', {
        currentTime: videoEl.currentTime,
        readyState: videoEl.readyState,
      })
    }
  }

  // Playlist refreshes are network activity, not proof that a frame decoded.
  videoEl.addEventListener('timeupdate', markPlaybackProgress)
  videoEl.addEventListener('playing', markPlaybackProgress)
  videoEl.addEventListener('loadeddata', markPlaybackProgress)
  videoEl.addEventListener('canplay', markPlaybackProgress)
  videoEl.addEventListener('canplaythrough', markPlaybackProgress)

  hls.on(Hls.Events.MANIFEST_PARSED, () => {
    console.debug('HLS manifest parsed, starting playback')
    reportDebug('info', 'manifest-parsed')
    markNetworkSuccess()
    // Kick playback in case autoplay is blocked in some environments.
    videoEl
      .play()
      .catch((err) =>
        console.warn('[HLS] play() after manifest parsed failed', err),
      )
  })

  hls.on(Hls.Events.LEVEL_LOADED, markNetworkSuccess)
  hls.on(Hls.Events.FRAG_LOADED, markNetworkSuccess)
  hls.on(Hls.Events.FRAG_CHANGED, markNetworkSuccess)
  hls.on(Hls.Events.BUFFER_APPENDED, markNetworkSuccess)

  const serializeHlsError = (data: any) => ({
    type: data.type,
    details: data.details,
    fatal: data.fatal,
    reason: (data as any)?.reason,
    errorMessage:
      typeof data.error === 'object'
        ? ((data.error as any)?.message ?? String(data.error))
        : data.error,
    errorCode: (data as any)?.error?.code,
    responseCode: (data as any)?.response?.code,
    responseText: (data as any)?.response?.text,
    url: (data as any)?.url ?? (data as any)?.context?.url,
  })

  const nonfatalErrorReporter = new CoalescingReporter<
    ReturnType<typeof serializeHlsError>
  >({
    windowMs: NONFATAL_LOG_WINDOW_MS,
    report: ({ value, occurrences, suppressed, coalescedWindowMs }) => {
      reportDebug('warning', 'hls-error', {
        ...value,
        occurrences,
        suppressed,
        coalescedWindowMs,
      })
      if (suppressed > 0) {
        console.warn('[HLS] repeated nonfatal errors', {
          details: value.details,
          responseCode: value.responseCode,
          occurrences,
          coalescedWindowMs,
        })
      } else {
        console.warn('[HLS] nonfatal error', {
          details: value.details,
          responseCode: value.responseCode,
        })
      }
    },
  })

  const reportFatal = (reason: string, detail?: unknown) => {
    if (reportedFatal) return
    reportedFatal = true
    nonfatalErrorReporter.flushAll()
    console.error('[HLS] giving up on stream', { src, reason, detail })
    try {
      window.postMessage(
        {
          type: 'streamwall:hls-fatal',
          reason,
          detail,
          src,
        },
        '*',
      )
    } catch (err) {
      console.warn('[HLS] failed to post fatal message', err)
    }

    try {
      hls.stopLoad()
      hls.destroy()
    } catch (err) {
      console.warn('[HLS] failed to teardown after fatal', err)
    }

    try {
      videoEl.pause()
    } catch (err) {
      console.warn('[HLS] failed to pause after fatal', err)
    }

    if (watchdog) {
      window.clearInterval(watchdog)
    }
    if (startupTimer) {
      window.clearTimeout(startupTimer)
      startupTimer = undefined
    }
    if (admissionTimer) {
      window.clearTimeout(admissionTimer)
      admissionTimer = undefined
    }
  }

  hls.on(Hls.Events.ERROR, (event, data) => {
    if (reportedFatal) return

    const serializedError = serializeHlsError(data)
    if (data.fatal) {
      console.error('[HLS] fatal error', serializedError)
      reportDebug('error', 'hls-error', serializedError)
    } else {
      const key = [
        serializedError.type,
        serializedError.details,
        serializedError.responseCode,
        serializedError.errorCode,
      ].join('|')
      nonfatalErrorReporter.add(key, serializedError)
    }

    // hls.js may promote a non-retryable fragment 404 to fatal before this
    // listener runs. Handle this known rolling-window condition first so it
    // resynchronizes rather than consuming the fatal-error budget.
    if (isExpiredMediaSegmentError(serializedError)) {
      const now = performance.now()
      // One failed fragment can produce closely-spaced loader events. A short
      // guard prevents them from repeatedly stopping a resync already in
      // progress while still reacting quickly to a genuinely stale request.
      if (now - lastExpiredSegmentResyncAt >= 2_000) {
        lastExpiredSegmentResyncAt = now
        const liveSyncPosition = hls.liveSyncPosition
        reportDebug('warning', 'expired-segment-resync', {
          url: serializedError.url,
          responseCode: serializedError.responseCode,
          currentTime: videoEl.currentTime,
          liveSyncPosition,
        })
        try {
          hls.stopLoad()
          if (
            typeof liveSyncPosition === 'number' &&
            Number.isFinite(liveSyncPosition) &&
            liveSyncPosition >= 0
          ) {
            videoEl.currentTime = liveSyncPosition
            hls.startLoad(liveSyncPosition)
          } else {
            // This is primarily a startup fallback, before the latency
            // controller has calculated a live position.
            hls.startLoad()
          }
        } catch (err) {
          console.warn('[HLS] expired segment resync failed', err)
        }
      }
      return
    }

    if (data.fatal) {
      fatalErrorCount += 1
      if (fatalErrorCount >= MAX_FATAL_ERRORS) {
        reportFatal('fatal-error', serializeHlsError(data))
        return
      }

      console.error('Fatal HLS error, recovering...')
      if (data.type === Hls.ErrorTypes.NETWORK_ERROR) {
        hls.startLoad()
      } else if (data.type === Hls.ErrorTypes.MEDIA_ERROR) {
        hls.recoverMediaError()
      }
      return
    }

    // Non-fatal errors should not count toward the fatal threshold
    fatalErrorCount = 0
  })

  const watchdog = window.setInterval(() => {
    const now = performance.now()
    const stalledMs = now - lastProgressTs

    // The startup timer owns the no-playback case. Stall recovery starts only
    // after the stream has demonstrated that it can render media.
    if (!playbackStarted) return

    if (stalledMs > STALL_RECOVERY_MS) {
      stallStartedAt ??= lastProgressTs
      const totalStallMs = now - stallStartedAt
      const current = videoEl.currentTime
      const bufferedAheadSeconds = getBufferedAheadSeconds(
        videoEl.buffered,
        current,
      )
      const recoveryAction = selectStallRecoveryAction({
        bufferedAheadSeconds,
        totalStallMs,
        hardReloadAttempted,
      })
      console.warn('[HLS] playback stall detected', {
        stalledMs: Math.round(stalledMs),
        totalStallMs: Math.round(totalStallMs),
        currentTime: current,
        lastTime,
        bufferedAheadSeconds,
        recoveryAction,
      })

      if (recoveryAction === 'fail') {
        reportFatal('stall-timeout', {
          stalledMs: Math.round(totalStallMs),
          lastTime,
          bufferedAheadSeconds: Math.round(bufferedAheadSeconds * 10) / 10,
        })
        return
      }

      if (now >= nextRecoveryAttemptAt) {
        nextRecoveryAttemptAt = now + getJitteredRecoveryIntervalMs()
        reportDebug('warning', 'stall-recovery', {
          stalledMs: Math.round(totalStallMs),
          currentTime: videoEl.currentTime,
          bufferedAheadSeconds: Math.round(bufferedAheadSeconds * 10) / 10,
          recoveryAction,
        })
        try {
          if (recoveryAction === 'nudge-buffered-media') {
            videoEl.currentTime = Math.min(
              current + BUFFERED_RECOVERY_NUDGE_SECONDS,
              current + Math.max(0, bufferedAheadSeconds - 0.1),
            )
          } else {
            if (recoveryAction === 'restart-loading') {
              hardReloadAttempted = true
              console.warn('[HLS] hard reload after prolonged unbuffered stall')
              reportDebug('warning', 'stall-hard-reload', {
                stalledMs: Math.round(totalStallMs),
                currentTime: videoEl.currentTime,
                bufferedAheadSeconds: Math.round(bufferedAheadSeconds * 10) / 10,
                recoveryAction,
              })
              hls.stopLoad()
            }
            hls.startLoad()
          }
          videoEl
            .play()
            .catch((err) =>
              console.warn('[HLS] play() after stall failed', err),
            )
        } catch (err) {
          console.warn('[HLS] recover after stall failed', err)
        }
      }
    }
  }, 5000)

  const reportStartupFailure = (elapsedMs: number) => {
    if (playbackStarted) return
    reportFatal('startup-timeout', {
      elapsedMs,
      readyState: videoEl.readyState,
    })
  }

  const startStartupTimer = () => {
    if (startupTimer || playbackStarted || reportedFatal) return
    startupTimer = window.setTimeout(() => {
      startupTimer = undefined
      if (playbackStarted) return

      if (shouldGrantStartupGrace(playbackStarted, videoEl.readyState)) {
        reportDebug('warning', 'startup-grace', {
          elapsedMs: TRAFFIC_CAMERA_STARTUP_FAILURE_MS,
          readyState: videoEl.readyState,
        })
        startupTimer = window.setTimeout(() => {
          startupTimer = undefined
          reportStartupFailure(
            TRAFFIC_CAMERA_STARTUP_FAILURE_MS +
              TRAFFIC_CAMERA_STARTUP_GRACE_MS,
          )
        }, TRAFFIC_CAMERA_STARTUP_GRACE_MS)
        return
      }

      reportStartupFailure(TRAFFIC_CAMERA_STARTUP_FAILURE_MS)
    }, TRAFFIC_CAMERA_STARTUP_FAILURE_MS)
  }

  const handleFirstRequestAdmission = (event: MessageEvent) => {
    if (event.source !== window) return
    const data = event.data as { type?: unknown } | null
    if (data?.type !== 'streamwall:hls-request-admitted') return
    if (admissionTimer) {
      window.clearTimeout(admissionTimer)
      admissionTimer = undefined
    }
    startStartupTimer()
  }
  window.addEventListener('message', handleFirstRequestAdmission)
  admissionTimer = window.setTimeout(() => {
    admissionTimer = undefined
    reportFatal('request-admission-timeout', {
      elapsedMs: TRAFFIC_CAMERA_REQUEST_ADMISSION_FAILURE_MS,
    })
  }, TRAFFIC_CAMERA_REQUEST_ADMISSION_FAILURE_MS)

  window.addEventListener('beforeunload', () => {
    nonfatalErrorReporter.clear()
    if (watchdog) {
      window.clearInterval(watchdog)
    }
    if (startupTimer) {
      window.clearTimeout(startupTimer)
    }
    if (admissionTimer) {
      window.clearTimeout(admissionTimer)
    }
    window.removeEventListener('message', handleFirstRequestAdmission)
    hls.destroy()
  })

  hls.attachMedia(videoEl)
  reportDebug('info', 'loading', startupDetail)
  hls.loadSource(src)

  console.debug('Loading HLS stream:', src)
}
