import Hls from 'hls.js'
import { CropRect } from 'streamwall-shared'

// Parse src immediately on load
const searchParams = new URLSearchParams(location.search)
const src = searchParams.get('src')
const cropParam = searchParams.get('crop')

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

  const scale = Math.max(containerWidth / crop.width, containerHeight / crop.height)
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
const updateProgress = () => {
  lastProgressTs = performance.now()
  lastTime = videoEl.currentTime
  fatalErrorCount = 0
}
videoEl.addEventListener('timeupdate', updateProgress)
videoEl.addEventListener('playing', updateProgress)
videoEl.addEventListener('loadeddata', updateProgress)
videoEl.addEventListener('loadedmetadata', updateProgress)
videoEl.addEventListener('canplay', updateProgress)
videoEl.addEventListener('canplaythrough', updateProgress)
videoEl.addEventListener('loadedmetadata', applyCrop)
window.addEventListener('resize', applyCrop)

// Start HLS loading immediately if src exists
if (src) {
  const MAX_FATAL_ERRORS = 3
  const STALL_FAILURE_MS = 90_000
  const STARTUP_FAILURE_MS = 10_000

  let watchdog: number | undefined
  let startupTimer: number | undefined
  let reportedFatal = false
  const startAt = performance.now()

  const hls = new Hls({
    lowLatencyMode: true,
    enableWorker: true,
    startLevel: -1, // Auto-select quality
    defaultAudioCodec: undefined,
    // Trim buffers to reduce memory/CPU churn when many streams run
    maxBufferLength: 8,
    maxMaxBufferLength: 16,
    backBufferLength: 30,
    maxBufferSize: 60 * 1000 * 1000,
    liveSyncDurationCount: 3,
    capLevelToPlayerSize: true,
    liveDurationInfinity: true,
  })

  hls.on(Hls.Events.MANIFEST_PARSED, () => {
    console.debug('HLS manifest parsed, starting playback')
    updateProgress()
    // Kick playback in case autoplay is blocked in some environments.
    videoEl
      .play()
      .catch((err) => console.warn('[HLS] play() after manifest parsed failed', err))
  })

  hls.on(Hls.Events.LEVEL_LOADED, updateProgress)
  hls.on(Hls.Events.FRAG_LOADED, updateProgress)
  hls.on(Hls.Events.FRAG_CHANGED, updateProgress)
  hls.on(Hls.Events.BUFFER_APPENDED, updateProgress)

  const serializeHlsError = (data: any) => ({
    type: data.type,
    details: data.details,
    fatal: data.fatal,
    reason: (data as any)?.reason,
    errorMessage:
      typeof data.error === 'object'
        ? (data.error as any)?.message ?? String(data.error)
        : data.error,
    errorCode: (data as any)?.error?.code,
  })

  const reportFatal = (reason: string, detail?: unknown) => {
    if (reportedFatal) return
    reportedFatal = true
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
  }

  hls.on(Hls.Events.ERROR, (event, data) => {
    console.error('HLS error:', data)
    if (reportedFatal) return

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

  hls.attachMedia(videoEl)
  hls.loadSource(src)

  watchdog = window.setInterval(() => {
    const now = performance.now()
    const stalledMs = now - lastProgressTs

    // If we never actually advanced playback time, bail out after 20s.
    if (!reportedFatal && lastTime === 0 && now - startAt > 20_000) {
      reportFatal('no-playback', {
        elapsedMs: Math.round(now - startAt),
        lastTime,
      })
      return
    }

    // If playback has stalled for >15s, attempt a gentle recover; after 45s hard-reload the stream
    if (stalledMs > 15000) {
      const current = videoEl.currentTime
      console.warn('[HLS] playback stall detected', {
        stalledMs: Math.round(stalledMs),
        currentTime: current,
        lastTime,
      })
      try {
        hls.startLoad()
        hls.recoverMediaError()
        videoEl.play().catch((err) => console.warn('[HLS] play() after stall failed', err))
      } catch (err) {
        console.warn('[HLS] recover after stall failed', err)
      }
      if (stalledMs > STALL_FAILURE_MS) {
        reportFatal('stall-timeout', {
          stalledMs: Math.round(stalledMs),
          lastTime,
        })
        return
      }
      if (stalledMs > 45000) {
        console.warn('[HLS] hard reload after prolonged stall')
        try {
          hls.stopLoad()
          hls.startLoad()
        } catch (err) {
          console.warn('[HLS] hard reload failed', err)
        }
        updateProgress()
      }
    }
  }, 5000)
  startupTimer = window.setTimeout(() => {
    const noProgressMs = performance.now() - lastProgressTs
    if (noProgressMs >= STARTUP_FAILURE_MS) {
      reportFatal('startup-timeout', { elapsedMs: Math.round(noProgressMs) })
    }
  }, STARTUP_FAILURE_MS)
  window.addEventListener('beforeunload', () => {
    if (watchdog) {
      window.clearInterval(watchdog)
    }
    if (startupTimer) {
      window.clearTimeout(startupTimer)
    }
  })
  
  console.debug('Loading HLS stream:', src)
}


