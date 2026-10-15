import { contextBridge, ipcRenderer, webFrame } from 'electron'
import throttle from 'lodash/throttle'
import {
  ContentDisplayOptions,
  CropRect,
  HlsRequestAcquireInput,
  HlsRequestAcquireResult,
  HlsRequestReleaseInput,
} from 'streamwall-shared'

const SCAN_THROTTLE = 500

const isLocalHlsPlayer =
  location.pathname.endsWith('/playHLS.html') &&
  (location.protocol === 'file:' ||
    location.hostname === 'localhost' ||
    location.hostname === '127.0.0.1')

const SPOTLIGHT_BLUR_CLASS = 'streamwall-spotlight-blurred'
let spotlightBlurred = false
try {
  webFrame.insertCSS(
    `html.${SPOTLIGHT_BLUR_CLASS} { filter: blur(8px) !important; }`,
    { cssOrigin: 'user' },
  )
} catch (error) {
  // A cosmetic failure must not prevent the media preload from starting.
  console.warn('[spotlight] blur style failed', error)
}
const applySpotlightBlur = () =>
  document.documentElement?.classList.toggle(SPOTLIGHT_BLUR_CLASS, spotlightBlurred)
ipcRenderer.on('spotlight-blur', (_event, blurred: boolean) => {
  spotlightBlurred = blurred === true
  applySpotlightBlur()
})
document.addEventListener('DOMContentLoaded', applySpotlightBlur, { once: true })

if (isLocalHlsPlayer) {
  contextBridge.exposeInMainWorld('streamwallHlsRequests', {
    acquire: (request: HlsRequestAcquireInput) =>
      ipcRenderer.invoke(
        'hls-request-acquire',
        request,
      ) as Promise<HlsRequestAcquireResult>,
    cancel: (requestId: number) =>
      ipcRenderer.send('hls-request-cancel', { requestId }),
    release: (request: HlsRequestReleaseInput) =>
      ipcRenderer.send('hls-request-release', request),
  })
}

type RendererErrorMessage = {
  type: 'streamwall:hls-fatal'
  reason: string
  detail?: unknown
  src?: string | null
}

type RendererDebugMessage = {
  type: 'streamwall:hls-log'
  level: 'info' | 'warning' | 'error'
  event: string
  message?: string
  detail?: unknown
  src?: string | null
}

function forwardRendererError(ev: MessageEvent) {
  const data = ev.data as RendererErrorMessage | RendererDebugMessage
  if (!data || typeof data !== 'object') {
    return
  }
  if (data.type === 'streamwall:hls-fatal') {
    ipcRenderer.send('view-error', { error: data })
  } else if (data.type === 'streamwall:hls-log') {
    ipcRenderer.send('view-debug-log', { log: data })
  }
}

window.addEventListener('message', forwardRendererError)

const VIDEO_OVERRIDE_STYLE = `
  * {
    pointer-events: none;
    display: none !important;
    position: static !important;
    z-index: 0 !important;
  }
  html, body, video, audio {
    display: block !important;
    background: black !important;
  }
  html, body {
    overflow: hidden !important;
    background: black !important;
  }
  video, iframe.__video__, audio {
    display: block !important;
    position: fixed !important;
    left: 0 !important;
    right: 0 !important;
    top: 0 !important;
    bottom: 0 !important;
    width: 100vw !important;
    height: 100vh !important;
    object-fit: cover !important;
    transition: none !important;
    z-index: 999999 !important;
  }
  audio {
    z-index: 999998 !important;
  }
  .__video_parent__ {
    display: block !important;
  }
  video.__rot180__ {
    transform: rotate(180deg) !important;
  }
  /* For 90 degree rotations, we position the video with swapped width and height and rotate it into place.
     It's helpful to offset the video so the transformation is centered in the viewport center.
     We move the video top left corner to center of the page and then translate half the video dimensions up and left.
     Note that the width and height are swapped in the translate because the video starts with the side dimensions swapped. */
  video.__rot90__ {
    transform: translate(-50vh, -50vw) rotate(90deg) !important;
  }
  video.__rot270__ {
    transform: translate(-50vh, -50vw) rotate(270deg) !important;
  }
  video.__rot90__, video.__rot270__ {
    left: 50vw !important;
    top: 50vh !important;
    width: 100vh !important;
    height: 100vw !important;
  }
`

const NO_SCROLL_STYLE = `
  html, body {
    overflow: hidden !important;
  }
`

const sleep = (ms: number) =>
  new Promise<void>((resolve) => setTimeout(() => resolve(), ms))

async function ensurePlayback(video: HTMLMediaElement, attempts = 3) {
  for (let i = 0; i < attempts; i++) {
    try {
      const wasMuted = video.muted
      video.muted = true
      await video.play()
      video.muted = wasMuted
      return true
    } catch (err) {
      console.warn('video play blocked; retrying muted', err)
      await sleep(300)
    }
  }
  return false
}

function startMediaHealthSampling(video: HTMLVideoElement) {
  let sampledAt = performance.now()
  let currentTime = video.currentTime
  let quality = video.getVideoPlaybackQuality?.()
  let totalFrames = quality?.totalVideoFrames ?? 0
  let droppedFrames = quality?.droppedVideoFrames ?? 0

  const timer = window.setInterval(() => {
    try {
      const now = performance.now()
      const nextQuality = video.getVideoPlaybackQuality?.()
      let bufferedAheadSeconds = 0
      for (let i = 0; i < video.buffered.length; i++) {
        if (
          video.buffered.start(i) <= video.currentTime &&
          video.currentTime <= video.buffered.end(i)
        ) {
          bufferedAheadSeconds = video.buffered.end(i) - video.currentTime
          break
        }
      }

      ipcRenderer.send('view-media-health', {
        elapsedMs: Math.round(now - sampledAt),
        presentedFrames: nextQuality
          ? Math.max(0, nextQuality.totalVideoFrames - totalFrames)
          : null,
        droppedFrames: nextQuality
          ? Math.max(0, nextQuality.droppedVideoFrames - droppedFrames)
          : null,
        advancedSeconds: Math.max(0, video.currentTime - currentTime),
        bufferedAheadSeconds,
        readyState: video.readyState,
        paused: video.paused,
        playbackRate: video.playbackRate,
        width: video.videoWidth,
        height: video.videoHeight,
      })

      sampledAt = now
      currentTime = video.currentTime
      quality = nextQuality
      totalFrames = quality?.totalVideoFrames ?? 0
      droppedFrames = quality?.droppedVideoFrames ?? 0
    } catch (error) {
      console.warn('[media health] sample failed', error)
    }
  }, 10_000)
  window.addEventListener('beforeunload', () => window.clearInterval(timer), {
    once: true,
  })
}

function startPlaybackWatchdog(video: HTMLVideoElement) {
  let lastTime = video.currentTime
  let lastProgress = Date.now()
  const timer = window.setInterval(() => {
    const current = video.currentTime
    if (Math.abs(current - lastTime) >= 0.1) {
      lastTime = current
      lastProgress = Date.now()
      return
    }
    if (Date.now() - lastProgress >= 60_000) {
      window.clearInterval(timer)
      ipcRenderer.send('view-error', { error: 'Video made no playback progress for 60 seconds' })
    }
  }, 10_000)
  window.addEventListener('beforeunload', () => window.clearInterval(timer), { once: true })
}

function reportFirstPlaybackProgress(video: HTMLVideoElement) {
  let previousTime = video.currentTime
  const onTimeUpdate = () => {
    const currentTime = video.currentTime
    if (video.videoWidth > 0 && Math.abs(currentTime - previousTime) >= 0.1) {
      video.removeEventListener('timeupdate', onTimeUpdate)
      ipcRenderer.send('view-playback-started')
    }
    previousTime = currentTime
  }
  video.addEventListener('timeupdate', onTimeUpdate)
  window.addEventListener('beforeunload', () =>
    video.removeEventListener('timeupdate', onTimeUpdate), { once: true })
}

const pageReady = new Promise((resolve) =>
  document.addEventListener('DOMContentLoaded', resolve, { once: true }),
)

class RotationController {
  video: HTMLVideoElement
  siteRotation: number
  customRotation: number

  constructor(video: HTMLVideoElement) {
    this.video = video
    this.siteRotation = 0
    this.customRotation = 0
  }

  _update() {
    const rotation = (this.siteRotation + this.customRotation) % 360
    if (![0, 90, 180, 270].includes(rotation)) {
      console.warn('ignoring invalid rotation', rotation)
    }
    this.video.className = `__rot${rotation}__`
  }

  setSite(rotation = 0) {
    this.siteRotation = rotation
    this._update()
  }

  setCustom(rotation = 0) {
    this.customRotation = rotation
    this._update()
  }
}

class CropController {
  video: HTMLVideoElement
  crop?: CropRect
  resizeObserver: ResizeObserver

  constructor(video: HTMLVideoElement) {
    this.video = video
    this.resizeObserver = new ResizeObserver(() => this.apply())
    this.resizeObserver.observe(document.body)
  }

  setCrop(crop?: CropRect) {
    this.crop = crop
    this.apply()
  }

  apply() {
    const crop = this.crop
    if (!crop || !this.video.videoWidth || !this.video.videoHeight) {
      this.video.style.setProperty('width', '100vw', 'important')
      this.video.style.setProperty('height', '100vh', 'important')
      this.video.style.removeProperty('transform')
      this.video.style.removeProperty('transform-origin')
      this.video.style.setProperty('object-fit', 'contain', 'important')
      return
    }

    // Scale so the crop fully covers the container, then translate to the crop origin.
    const containerWidth = window.innerWidth
    const containerHeight = window.innerHeight
    if (!containerWidth || !containerHeight) {
      return
    }

    const scale = Math.max(
      containerWidth / crop.width,
      containerHeight / crop.height,
    )
    const targetWidth = this.video.videoWidth * scale
    const targetHeight = this.video.videoHeight * scale

    this.video.style.setProperty('width', `${targetWidth}px`, 'important')
    this.video.style.setProperty('height', `${targetHeight}px`, 'important')
    this.video.style.setProperty('object-fit', 'fill', 'important')
    this.video.style.setProperty('transform-origin', 'top left', 'important')
    this.video.style.setProperty(
      'transform',
      `translate(${-crop.x * scale}px, ${-crop.y * scale}px)`,
      'important',
    )
  }
}

// Watch for media tags and mute them as soon as possible.
async function lockdownMediaTags() {
  const lockdown = throttle(() => {
    webFrame.executeJavaScript(`
      for (const el of document.querySelectorAll('video, audio')) {
        if (el.__sw) {
          continue
        }
        // Prevent sites from re-muting the video
        Object.defineProperty(el, 'muted', { writable: true, value: false })
        // Prevent Facebook from pausing the video after page load.
        Object.defineProperty(el, 'pause', { writable: false, value: () => {} })
        el.__sw = true
      }
    `)
  }, SCAN_THROTTLE)
  await pageReady
  const observer = new MutationObserver(lockdown)
  observer.observe(document.body, { subtree: true, childList: true })
}

async function waitForQuery(query: string): Promise<Element> {
  console.log(`waiting for '${query}'...`)
  await pageReady
  return new Promise((resolve) => {
    const scan = throttle(() => {
      const el = document.querySelector(query)
      if (el) {
        console.log(`found '${query}'`)
        resolve(el)
        observer.disconnect()
      }
    }, SCAN_THROTTLE)

    const observer = new MutationObserver(scan)
    observer.observe(document.body, { subtree: true, childList: true })
    scan()
  })
}

async function waitForVideo(kind: 'video' | 'audio'): Promise<{
  video?: HTMLMediaElement
  iframe?: HTMLIFrameElement
}> {
  lockdownMediaTags()

  let video: Element | null | void = await Promise.race([
    waitForQuery(kind),
    sleep(10 * 1000),
  ])
  if (video instanceof HTMLMediaElement) {
    return { video }
  }

  let iframe
  for (iframe of document.querySelectorAll('iframe')) {
    video = iframe.contentDocument?.querySelector?.(kind)
    if (video instanceof HTMLVideoElement) {
      return { video, iframe }
    }
  }
  return {}
}

const igHacks = {
  isMatch() {
    return location.host === 'www.instagram.com'
  },
  async onLoad() {
    const playButton = await Promise.race([
      waitForQuery('button'),
      waitForQuery('video'),
      sleep(1000),
    ])
    if (
      playButton instanceof HTMLButtonElement &&
      playButton.tagName === 'BUTTON' &&
      playButton.textContent === 'Tap to play'
    ) {
      playButton.click()
    }
  },
}

async function findVideo(kind: 'video' | 'audio') {
  if (igHacks.isMatch()) {
    await igHacks.onLoad()
  }

  const { video, iframe } = await waitForVideo(kind)
  if (!video) {
    throw new Error('could not find video')
  }
  if (iframe && iframe.contentDocument) {
    // TODO: verify iframe still works
    const style = iframe.contentDocument.createElement('style')
    style.innerHTML = VIDEO_OVERRIDE_STYLE
    iframe.contentDocument.head.appendChild(style)
    iframe.className = '__video__'
    let parentEl = iframe.parentElement
    while (parentEl) {
      parentEl.className = '__video_parent__'
      parentEl = parentEl.parentElement
    }
    iframe.contentDocument.body.appendChild(video)
  } else {
    document.body.appendChild(video)
  }

  const startPlayback = async (reason: string) => {
    const ok = await ensurePlayback(video)
    if (!ok) {
      console.warn(`unable to start playback (${reason})`)
    }
    return ok
  }

  await startPlayback('initial')

  const resumePlayback = throttle((reason: string) => {
    if (video.paused || video.ended) {
      void startPlayback(reason)
    }
  }, 2000)

  video.addEventListener('stalled', () => resumePlayback('stalled'))
  video.addEventListener('suspend', () => resumePlayback('suspend'))
  video.addEventListener('ended', () => {
    video.currentTime = 0
    resumePlayback('ended')
  })

  if (video instanceof HTMLVideoElement && !video.videoWidth) {
    console.log(`video isn't playing yet. waiting for it to start...`)
    const videoReady = new Promise((resolve) =>
      video.addEventListener('playing', resolve, { once: true }),
    )
    await Promise.race([videoReady, sleep(30 * 1000)])
    if (!video.videoWidth) {
      console.warn('Timeout waiting for video to start; retrying')
      await startPlayback('post-timeout')
    } else {
      console.log('video started')
    }
  }

  const info = {
    title: document.title,
  }
  return { info, video }
}

async function main() {
  const viewInit = ipcRenderer.invoke('view-init')
  const pageReady = new Promise((resolve) => process.once('loaded', resolve))

  const [{ content, options: initialOptions, mediaHealthEnabled, nearbyFallbackEnabled }] = await Promise.all([
    viewInit,
    pageReady,
  ])

  let rotationController: RotationController | undefined
  let cropController: CropController | undefined
  if (
    content.kind === 'video' ||
    content.kind === 'audio' ||
    content.kind === 'hdhomerun'
  ) {
    // Avoid overriding layout for the built-in HLS player so it can manage crop/transform itself.
    if (!isLocalHlsPlayer) {
      webFrame.insertCSS(VIDEO_OVERRIDE_STYLE, { cssOrigin: 'user' })
    }
    const { info, video } = await findVideo(
      content.kind === 'hdhomerun' ? 'video' : content.kind,
    )
    if (
      (content.kind === 'video' || content.kind === 'hdhomerun') &&
      video instanceof HTMLVideoElement
    ) {
      rotationController = new RotationController(video)
      cropController = new CropController(video)
      if (mediaHealthEnabled) {
        startMediaHealthSampling(video)
      }
      if (nearbyFallbackEnabled) {
        reportFirstPlaybackProgress(video)
      }
      if (nearbyFallbackEnabled && !isLocalHlsPlayer) {
        startPlaybackWatchdog(video)
      }
    }
    ipcRenderer.send('view-info', { info })
  } else if (content.kind === 'web') {
    webFrame.insertCSS(NO_SCROLL_STYLE, { cssOrigin: 'user' })
  }

  ipcRenderer.send('view-loaded')

  function updateOptions(options: ContentDisplayOptions) {
    if (rotationController) {
      rotationController.setCustom(options.rotation)
    }
    if (cropController) {
      cropController.setCrop(options.crop)
    }
  }
  ipcRenderer.on('options', (ev, options) => updateOptions(options))
  updateOptions(initialOptions)
}

main().catch((error) => {
  ipcRenderer.send('view-error', { error })
})
