import assert from 'assert'
import { app, BrowserWindow, ipcMain, Rectangle, WebContents, WebContentsView } from 'electron'
import EventEmitter from 'events'
import fs from 'fs'
import intersection from 'lodash/intersection'
import isEqual from 'lodash/isEqual'
import path from 'path'
import {
  boxesFromViewContentMap,
  ContentDisplayOptions,
  HlsRequestAcquireInput,
  HlsRequestAcquireResult,
  HlsRequestKind,
  HlsRequestOutcome,
  HlsRequestReleaseInput,
  StreamData,
  StreamList,
  StreamwallDebugLogEntry,
  StreamwallState,
  StreamWindowConfig,
  ViewContent,
  ViewContentMap,
  ViewPos,
  ViewState,
} from 'streamwall-shared'
import { createActor, EventFrom, SnapshotFrom } from 'xstate'
import {
  getMediaSessionPartition,
  HLS_MEDIA_SESSION_POOL_COUNT,
  isIowaDotHlsURL,
} from './hlsMediaSessionPool'
import {
  hlsRequestScheduler,
  type HlsRequestSchedulerEvent,
} from './hlsRequestScheduler'
import { hlsStartupScheduler } from './hlsStartupScheduler'
import { loadHTML } from './loadHTML'
import { createRuntimeDiagnostics } from './runtimeDiagnostics'
import viewStateMachine, { ViewActor } from './viewStateMachine'

const DEBUG_LOG_LIMIT = 250
const MEDIA_HEALTH_LOG_LIMIT_BYTES = 8 * 1024 * 1024
const MEDIA_HEALTH_FRESH_MS = 25_000
const SPOTLIGHT_ANIMATION_MS = 300
const SPOTLIGHT_START_TIMEOUT_MS = 75_000

interface MediaHealthSample {
  sampledAt: number
  elapsedMs: number
  presentedFrames: number | null
  droppedFrames: number | null
  advancedSeconds: number
  bufferedAheadSeconds: number
  readyState: number
  paused: boolean
  playbackRate: number
  width: number
  height: number
}

function parseMediaHealthSample(value: unknown): MediaHealthSample | undefined {
  const sample = asRecord(value)
  if (!sample) return undefined
  const bounded = (key: string, max: number) => {
    const number = sample[key]
    return typeof number === 'number' && Number.isFinite(number) &&
      number >= 0 && number <= max ? number : undefined
  }
  const elapsedMs = bounded('elapsedMs', 120_000)
  const presentedFrames = sample.presentedFrames === null
    ? null : bounded('presentedFrames', 100_000)
  const droppedFrames = sample.droppedFrames === null
    ? null : bounded('droppedFrames', 100_000)
  const advancedSeconds = bounded('advancedSeconds', 10_000)
  const bufferedAheadSeconds = bounded('bufferedAheadSeconds', 3_600)
  const readyState = bounded('readyState', 4)
  const playbackRate = bounded('playbackRate', 16)
  const width = bounded('width', 16_384)
  const height = bounded('height', 16_384)
  if (
    elapsedMs === undefined || elapsedMs === 0 || presentedFrames === undefined ||
    droppedFrames === undefined || advancedSeconds === undefined ||
    bufferedAheadSeconds === undefined || readyState === undefined ||
    playbackRate === undefined ||
    width === undefined || height === undefined ||
    typeof sample.paused !== 'boolean'
  ) return undefined
  return {
    sampledAt: Date.now(), elapsedMs, presentedFrames, droppedFrames,
    advancedSeconds, bufferedAheadSeconds, readyState,
    paused: sample.paused, playbackRate, width, height,
  }
}
const HLS_REQUEST_TELEMETRY_WINDOW_MS = 10_000
const DEBUG_DETAIL_KEYS = new Set([
  'type',
  'details',
  'fatal',
  'reason',
  'errorMessage',
  'errorCode',
  'responseCode',
  'elapsedMs',
  'readyState',
  'stalledMs',
  'totalStallMs',
  'lastTime',
  'currentTime',
  'bufferedAheadSeconds',
  'recoveryAction',
  'activeStartups',
  'queuedStartups',
  'queueWaitMs',
  'occurrences',
  'suppressed',
  'coalescedWindowMs',
])

function asRecord(value: unknown): Record<string, unknown> | undefined {
  if (value == null || typeof value !== 'object' || Array.isArray(value)) {
    return undefined
  }
  return value as Record<string, unknown>
}

function truncateDebugText(value: string, maxLength = 500) {
  return value.length <= maxLength ? value : `${value.slice(0, maxLength - 1)}…`
}

function redactDebugURL(value: unknown) {
  if (typeof value !== 'string' || value.length === 0) return undefined
  try {
    const url = new URL(value)
    url.username = ''
    url.password = ''
    url.search = ''
    url.hash = ''
    return truncateDebugText(url.toString())
  } catch {
    // Do not expose an unparseable value that might contain a credential.
    return undefined
  }
}

function sanitizeDebugDetail(value: unknown) {
  const source = asRecord(value)
  if (!source) return undefined

  const detail: Record<string, string | number | boolean | null> = {}
  for (const [key, item] of Object.entries(source)) {
    if (!DEBUG_DETAIL_KEYS.has(key)) continue
    if (
      item === null ||
      typeof item === 'number' ||
      typeof item === 'boolean'
    ) {
      detail[key] = item
    } else if (typeof item === 'string') {
      detail[key] = truncateDebugText(item)
    }
  }
  return Object.keys(detail).length > 0 ? detail : undefined
}

function getDisplayOptions(stream: StreamData): ContentDisplayOptions {
  if (!stream) {
    return {}
  }
  const opts: ContentDisplayOptions = {}
  if (typeof stream.rotation === 'number') {
    opts.rotation = stream.rotation
  }
  if (stream.crop) {
    opts.crop = stream.crop
  }
  return opts
}

function pickStreamByUrl(streams: StreamList, url: string) {
  const candidates = streams.byURL?.get(url)
  if (!candidates || candidates.length === 0) return undefined
  // Prefer a candidate without crop; otherwise fall back to the first entry.
  return candidates.find((s) => !s.crop) ?? candidates[0]
}

interface HlsRequestTelemetryBucket {
  startedAt: number
  requests: number
  admittedRequests: number
  rejectedRequests: number
  completedRequests: number
  errorRequests: number
  timeoutRequests: number
  abortedRequests: number
  queueWaitSamples: number
  totalQueueWaitMs: number
  activeTimeSamples: number
  totalActiveMs: number
  maxQueueWaitMs: number
  maxQueueDepth: number
  maxHostQueueDepth: number
  maxActiveHostRequests: number
  sampleURL?: string
}

const HLS_REQUEST_KINDS = new Set<HlsRequestKind>([
  'manifest',
  'playlist',
  'fragment',
  'key',
  'other',
])
const HLS_REQUEST_OUTCOMES = new Set<HlsRequestOutcome>([
  'completed',
  'error',
  'timeout',
  'aborted',
])

function parseHlsAcquireInput(value: unknown): HlsRequestAcquireInput | undefined {
  const source = asRecord(value)
  if (!source) return undefined
  const { requestId, url, kind } = source
  if (
    !Number.isSafeInteger(requestId) ||
    typeof url !== 'string' ||
    typeof kind !== 'string' ||
    !HLS_REQUEST_KINDS.has(kind as HlsRequestKind)
  ) {
    return undefined
  }
  return { requestId: requestId as number, url, kind: kind as HlsRequestKind }
}

function parseHlsReleaseInput(value: unknown): HlsRequestReleaseInput | undefined {
  const source = asRecord(value)
  if (!source) return undefined
  const { permitId, outcome } = source
  if (
    !Number.isSafeInteger(permitId) ||
    typeof outcome !== 'string' ||
    !HLS_REQUEST_OUTCOMES.has(outcome as HlsRequestOutcome)
  ) {
    return undefined
  }
  return {
    permitId: permitId as number,
    outcome: outcome as HlsRequestOutcome,
  }
}

export interface StreamWindowEventMap {
  load: []
  close: []
  state: [ViewState[]]
  'spotlight-changed': [string | null]
}

export default class StreamWindow extends EventEmitter<StreamWindowEventMap> {
  config: StreamWindowConfig
  win: BrowserWindow
  backgroundView: WebContentsView
  overlayView: WebContentsView
  views: Map<number, ViewActor>
  private probeViews = new Map<number, ViewActor>()
  private promotableProbeIds = new Set<number>()
  private probePlaybackStarted = new Set<number>()
  private probePlaybackWaiters = new Map<number, () => void>()
  refreshInProgress: Promise<void> | null = null
  debugLogs: StreamwallDebugLogEntry[] = []
  nextDebugLogId = 1
  hlsRequestTelemetry = new Map<string, HlsRequestTelemetryBucket>()
  mediaHealth = new Map<number, MediaHealthSample>()
  private spotlightToken: string | null = null
  private spotlightActor: ViewActor | null = null
  private spotlightSourceId: number | null = null
  private spotlightSourceWasBlurred = false
  private spotlightPhase: 'preparing' | 'open' | 'closing' | null = null
  private spotlightSubscription?: { unsubscribe(): void }
  private spotlightStartTimer?: NodeJS.Timeout
  private spotlightAnimation?: NodeJS.Timeout
  private spotlightAnimationGeneration = 0
  static ipcRegistered = false
  static senderToWindow = new Map<number, StreamWindow>()
  static nextHlsMediaSessionPool = 0
  static mediaHealthEnabled = false
  static nearbyFallbackEnabled = false
  static mediaHealthWindows = new Set<StreamWindow>()
  static mediaHealthWrite = Promise.resolve()

  static configureMediaHealth(enabled: boolean) {
    if (!enabled || StreamWindow.mediaHealthEnabled) return
    StreamWindow.mediaHealthEnabled = true
    const logPath = path.join(app.getPath('userData'), 'media-health.jsonl')
    console.log('[media health] writing snapshots to', logPath)
    setInterval(() => {
      const now = Date.now()
      const grids = Array.from(StreamWindow.mediaHealthWindows, (grid) =>
        grid.getMediaHealthSnapshot(now),
      )
      const line = JSON.stringify({ timestamp: new Date(now).toISOString(), grids }) + '\n'
      StreamWindow.mediaHealthWrite = StreamWindow.mediaHealthWrite
        .then(async () => {
          const size = await fs.promises.stat(logPath)
            .then((stat) => stat.size, () => 0)
          await fs.promises.writeFile(logPath, line, {
            flag: size >= MEDIA_HEALTH_LOG_LIMIT_BYTES ? 'w' : 'a',
          })
        })
        .catch((error) => console.warn('[media health] log write failed', error))
    }, 30_000).unref()
  }

  constructor(config: StreamWindowConfig) {
    super()
    this.config = config
    this.views = new Map()

    StreamWindow.ensureIPCHandlers()

    const { width, height, x, y, frameless, fullscreen, backgroundColor } =
      this.config
    const win = new BrowserWindow({
      title: 'Streamwall',
      width,
      height,
      x,
      y,
      frame: !frameless,
      fullscreen: !!fullscreen,
      backgroundColor,
      useContentSize: true,
      show: false,
    })
    console.log('[grid] window created', {
      width,
      height,
      x,
      y,
      frameless,
      fullscreen,
    })
    win.removeMenu()
    win.loadURL('about:blank')
    win.on('ready-to-show', () =>
      console.log('[grid] ready-to-show', {
        x: win.getBounds().x,
        y: win.getBounds().y,
      }),
    )
    win.on('show', () => console.log('[grid] show'))
    win.on('closed', () => console.log('[grid] closed'))
    win.on('close', () => {
      this.finishSpotlight()
      this.emit('close')
    })
    win.on('unresponsive', () => console.warn('[grid] unresponsive'))
    win.webContents.on('render-process-gone', (_e, details) => {
      console.warn('[grid] render-process-gone', details)
    })
    win.webContents.on(
      'did-fail-load',
      (_e, errCode, errDesc, validatedURL) => {
        console.warn('[grid] did-fail-load', { errCode, errDesc, validatedURL })
      },
    )
    win.webContents.on('did-finish-load', () => {
      console.log('[grid] did-finish-load')
    })

    // Work around https://github.com/electron/electron/issues/14308
    // via https://github.com/lutzroeder/netron/commit/910ce67395130690ad76382c094999a4f5b51e92
    win.once('ready-to-show', () => {
      win.resizable = false
      win.show()
    })
    this.win = win
    if (StreamWindow.mediaHealthEnabled) {
      StreamWindow.mediaHealthWindows.add(this)
      win.on('closed', () => StreamWindow.mediaHealthWindows.delete(this))
    }

    const backgroundView = new WebContentsView({
      webPreferences: {
        preload: path.join(__dirname, 'layerPreload.js'),
      },
    })
    backgroundView.setBackgroundColor('#0000')
    win.contentView.addChildView(backgroundView)
    backgroundView.setBounds({
      x: 0,
      y: 0,
      width,
      height,
    })
    loadHTML(backgroundView.webContents, 'background')
    this.backgroundView = backgroundView

    const overlayView = new WebContentsView({
      webPreferences: {
        contextIsolation: true,
        preload: path.join(__dirname, 'layerPreload.js'),
      },
    })
    overlayView.setBackgroundColor('#0000')
    win.contentView.addChildView(overlayView)
    overlayView.setBounds({
      x: 0,
      y: 0,
      width,
      height,
    })
    loadHTML(overlayView.webContents, 'overlay')
    this.overlayView = overlayView

    const syncBounds = () => {
      const bounds = win.getContentBounds()
      backgroundView.setBounds({
        x: 0,
        y: 0,
        width: bounds.width,
        height: bounds.height,
      })
      overlayView.setBounds({
        x: 0,
        y: 0,
        width: bounds.width,
        height: bounds.height,
      })
      this.applySpotlight()
    }
    win.on('resize', syncBounds)
    syncBounds()
    if (fullscreen) {
      win.once('ready-to-show', syncBounds)
    }

    StreamWindow.registerSender(this.backgroundView.webContents.id, this)
    StreamWindow.registerSender(this.overlayView.webContents.id, this)
  }

  static ensureIPCHandlers() {
    if (StreamWindow.ipcRegistered) return
    StreamWindow.ipcRegistered = true

    ipcMain.handle('layer:load', (ev) => {
      const inst = StreamWindow.senderToWindow.get(ev.sender.id)
      if (!inst) return
      if (
        ev.sender !== inst.backgroundView.webContents &&
        ev.sender !== inst.overlayView.webContents
      ) {
        return
      }
      inst.emit('load')
    })

    ipcMain.on(
      'spotlight-local',
      (ev, { url, streamId }: { url?: string; streamId?: string }) => {
        const inst = StreamWindow.senderToWindow.get(ev.sender.id)
        if (!inst || ev.sender !== inst.overlayView.webContents) return
        inst.toggleSpotlight(url, streamId)
      },
    )

    ipcMain.handle('view-init', async (ev) => {
      const inst = StreamWindow.senderToWindow.get(ev.sender.id)
      if (!inst) return
      const view = inst.findActor(ev.sender.id)
      if (view) {
        view.send({ type: 'VIEW_INIT' })
        const { content, options } = view.getSnapshot().context
        return {
          content, options,
          mediaHealthEnabled: StreamWindow.mediaHealthEnabled,
          nearbyFallbackEnabled: StreamWindow.nearbyFallbackEnabled,
        }
      }
    })

    ipcMain.handle(
      'hls-request-acquire',
      async (ev, value: unknown): Promise<HlsRequestAcquireResult> => {
        const inst = StreamWindow.senderToWindow.get(ev.sender.id)
        if (!inst || !inst.findActor(ev.sender.id)) {
          return {
            admitted: false,
            reason: 'unauthorized',
            queueWaitMs: 0,
          }
        }

        const request = parseHlsAcquireInput(value)
        if (!request) {
          return {
            admitted: false,
            reason: 'invalid-request',
            queueWaitMs: 0,
          }
        }

        let url: URL
        try {
          url = new URL(request.url)
        } catch {
          return {
            admitted: false,
            reason: 'invalid-request',
            queueWaitMs: 0,
          }
        }
        if (url.protocol !== 'http:' && url.protocol !== 'https:') {
          return {
            admitted: false,
            reason: 'invalid-request',
            queueWaitMs: 0,
          }
        }

        return hlsRequestScheduler.acquire({
          requestId: request.requestId,
          rendererId: ev.sender.id,
          host: url.host,
          kind: request.kind,
        })
      },
    )

    ipcMain.on('hls-request-cancel', (ev, value: unknown) => {
      const inst = StreamWindow.senderToWindow.get(ev.sender.id)
      const source = asRecord(value)
      const requestId = source?.requestId
      if (!inst || !inst.findActor(ev.sender.id)) return
      if (!Number.isSafeInteger(requestId)) return
      hlsRequestScheduler.cancel(ev.sender.id, requestId as number)
    })

    ipcMain.on('hls-request-release', (ev, value: unknown) => {
      const inst = StreamWindow.senderToWindow.get(ev.sender.id)
      const request = parseHlsReleaseInput(value)
      if (!inst || !inst.findActor(ev.sender.id) || !request) return
      hlsRequestScheduler.release(
        ev.sender.id,
        request.permitId,
        request.outcome,
      )
    })

    ipcMain.on('view-loaded', (ev) => {
      hlsStartupScheduler.release(ev.sender.id)
      StreamWindow.senderToWindow
        .get(ev.sender.id)
        ?.findActor(ev.sender.id)
        ?.send?.({ type: 'VIEW_LOADED' })
    })

    ipcMain.on('view-info', (ev, { info }) => {
      StreamWindow.senderToWindow
        .get(ev.sender.id)
        ?.findActor(ev.sender.id)
        ?.send?.({ type: 'VIEW_INFO', info })
    })

    ipcMain.on('view-error', (ev, { error }) => {
      const inst = StreamWindow.senderToWindow.get(ev.sender.id)
      if (!inst) return
      hlsStartupScheduler.release(ev.sender.id)
      inst.recordRendererLog(ev.sender.id, error)
      inst.findActor(ev.sender.id)?.send?.({ type: 'VIEW_ERROR', error })
    })

    ipcMain.on('view-debug-log', (ev, { log }) => {
      StreamWindow.senderToWindow
        .get(ev.sender.id)
        ?.recordRendererLog(ev.sender.id, log)
    })

    ipcMain.on('view-playback-started', (ev) => {
      StreamWindow.senderToWindow.get(ev.sender.id)?.markProbePlaybackStarted(ev.sender.id)
    })

    ipcMain.on('view-media-health', (ev, value: unknown) => {
      if (!StreamWindow.mediaHealthEnabled) return
      const inst = StreamWindow.senderToWindow.get(ev.sender.id)
      if (!inst || !inst.views.has(ev.sender.id)) return
      const sample = parseMediaHealthSample(value)
      if (sample) inst.mediaHealth.set(ev.sender.id, sample)
    })

    ipcMain.on('devtools-overlay', (ev) => {
      StreamWindow.senderToWindow
        .get(ev.sender.id)
        ?.overlayView.webContents.openDevTools()
    })

    ipcMain.on('view-expand', (ev, { url }) => {
      const inst = StreamWindow.senderToWindow.get(ev.sender.id)
      if (!inst || ev.sender !== inst.overlayView.webContents) {
        return
      }

      const { width, height } = inst.config
      let targetView: ViewActor | undefined
      let originalPos: ViewPos | null = null

      for (const view of inst.views.values()) {
        const snapshot = view.getSnapshot()
        if (snapshot.context.content?.url === url) {
          targetView = view
          originalPos = snapshot.context.pos
          break
        }
      }

      if (!targetView || !originalPos) {
        return
      }

      const content = targetView.getSnapshot().context.content
      if (!content) {
        return
      }

      if (url) {
        const expandedPos = {
          x: Math.floor(width * 0.16666),
          y: Math.floor(height * 0.16666),
          width: Math.floor(width * 0.66666),
          height: Math.floor(height * 0.66666),
          spaces: originalPos.spaces,
        }
        targetView.send({ type: 'DISPLAY', pos: expandedPos, content })
      } else {
        targetView.send({ type: 'DISPLAY', pos: originalPos, content })
      }
    })
  }

  static registerSender(id: number, inst: StreamWindow) {
    StreamWindow.senderToWindow.set(id, inst)
  }

  appendDebugLog(
    entry: Omit<StreamwallDebugLogEntry, 'id' | 'timestamp'>,
  ) {
    this.debugLogs.push({
      ...entry,
      id: this.nextDebugLogId++,
      timestamp: new Date().toISOString(),
    })
    if (this.debugLogs.length > DEBUG_LOG_LIMIT) {
      this.debugLogs.splice(0, this.debugLogs.length - DEBUG_LOG_LIMIT)
    }
  }

  recordHlsRequestEvent(senderId: number, event: HlsRequestSchedulerEvent) {
    const now = Date.now()
    let bucket = this.hlsRequestTelemetry.get(event.host)

    if (
      bucket &&
      now - bucket.startedAt >= HLS_REQUEST_TELEMETRY_WINDOW_MS
    ) {
      const windowMs = now - bucket.startedAt
      this.appendDebugLog({
        level:
          bucket.rejectedRequests > 0 || bucket.timeoutRequests > 0
            ? 'warning'
            : 'info',
        event: 'request-scheduler',
        message: `HLS scheduler admitted ${bucket.admittedRequests}/${bucket.requests} requests for ${event.host}; rejected ${bucket.rejectedRequests}`,
        url: bucket.sampleURL,
        detail: {
          host: event.host,
          windowMs,
          requestCount: bucket.requests,
          admittedRequestCount: bucket.admittedRequests,
          rejectedRequestCount: bucket.rejectedRequests,
          completedRequestCount: bucket.completedRequests,
          errorRequestCount: bucket.errorRequests,
          timeoutRequestCount: bucket.timeoutRequests,
          abortedRequestCount: bucket.abortedRequests,
          averageQueueWaitMs: Math.round(
            bucket.totalQueueWaitMs / Math.max(1, bucket.queueWaitSamples),
          ),
          averageActiveMs: Math.round(
            bucket.totalActiveMs / Math.max(1, bucket.activeTimeSamples),
          ),
          maxQueueWaitMs: bucket.maxQueueWaitMs,
          maxQueueDepth: bucket.maxQueueDepth,
          maxHostQueueDepth: bucket.maxHostQueueDepth,
          maxActiveHostRequests: bucket.maxActiveHostRequests,
        },
      })
      bucket = undefined
    }

    if (!bucket) {
      bucket = {
        startedAt: now,
        requests: 0,
        admittedRequests: 0,
        rejectedRequests: 0,
        completedRequests: 0,
        errorRequests: 0,
        timeoutRequests: 0,
        abortedRequests: 0,
        queueWaitSamples: 0,
        totalQueueWaitMs: 0,
        activeTimeSamples: 0,
        totalActiveMs: 0,
        maxQueueWaitMs: 0,
        maxQueueDepth: 0,
        maxHostQueueDepth: 0,
        maxActiveHostRequests: 0,
      }
      this.hlsRequestTelemetry.set(event.host, bucket)
    }

    const context = this.findActor(senderId)?.getSnapshot().context
    bucket.sampleURL ??= redactDebugURL(context?.content?.url)
    if (event.type === 'queued') {
      bucket.requests += 1
      bucket.maxQueueDepth = Math.max(
        bucket.maxQueueDepth,
        event.queueDepth,
      )
      bucket.maxHostQueueDepth = Math.max(
        bucket.maxHostQueueDepth,
        event.hostQueueDepth,
      )
    } else if (event.type === 'admitted') {
      bucket.admittedRequests += 1
      bucket.queueWaitSamples += 1
      bucket.totalQueueWaitMs += event.queueWaitMs
      bucket.maxQueueWaitMs = Math.max(
        bucket.maxQueueWaitMs,
        event.queueWaitMs,
      )
      bucket.maxQueueDepth = Math.max(
        bucket.maxQueueDepth,
        event.queueDepth,
      )
      bucket.maxHostQueueDepth = Math.max(
        bucket.maxHostQueueDepth,
        event.hostQueueDepth,
      )
      bucket.maxActiveHostRequests = Math.max(
        bucket.maxActiveHostRequests,
        event.activeHostRequests,
      )
    } else if (event.type === 'rejected') {
      bucket.rejectedRequests += Number(event.reason === 'queue-timeout')
      bucket.abortedRequests += Number(event.reason === 'aborted')
      bucket.queueWaitSamples += 1
      bucket.totalQueueWaitMs += event.queueWaitMs
      bucket.maxQueueWaitMs = Math.max(
        bucket.maxQueueWaitMs,
        event.queueWaitMs,
      )
      bucket.maxQueueDepth = Math.max(
        bucket.maxQueueDepth,
        event.queueDepth,
      )
      bucket.maxHostQueueDepth = Math.max(
        bucket.maxHostQueueDepth,
        event.hostQueueDepth,
      )
      bucket.maxActiveHostRequests = Math.max(
        bucket.maxActiveHostRequests,
        event.activeHostRequests,
      )
    } else {
      bucket.activeTimeSamples += 1
      bucket.totalActiveMs += event.activeMs
      bucket.completedRequests += Number(event.outcome === 'completed')
      bucket.errorRequests += Number(event.outcome === 'error')
      bucket.timeoutRequests += Number(event.outcome === 'timeout')
      bucket.abortedRequests += Number(event.outcome === 'aborted')
      bucket.maxActiveHostRequests = Math.max(
        bucket.maxActiveHostRequests,
        event.activeHostRequests + 1,
      )
    }
  }

  recordRendererLog(senderId: number, value: unknown) {
    const source = asRecord(value) ?? {}
    const view = this.findActor(senderId)
    const context = view?.getSnapshot().context
    const isFatal = source.type === 'streamwall:hls-fatal'
    const rawEvent = isFatal ? source.reason : source.event
    const event =
      typeof rawEvent === 'string' && rawEvent.length > 0
        ? truncateDebugText(rawEvent, 80)
        : 'view-error'
    const rawLevel = source.level
    const level: StreamwallDebugLogEntry['level'] = isFatal
      ? 'error'
      : rawLevel === 'warning' || rawLevel === 'error'
        ? rawLevel
        : 'info'
    const rawMessage = source.message
    const message =
      typeof rawMessage === 'string' && rawMessage.length > 0
        ? truncateDebugText(rawMessage)
        : isFatal
          ? `HLS stream failed: ${event}`
          : `HLS ${event.replace(/-/g, ' ')}`
    this.appendDebugLog({
      level,
      event,
      message,
      viewIdx: context?.pos?.spaces[0],
      streamId: context?.content?.streamId,
      url: redactDebugURL(source.src) ?? redactDebugURL(context?.content?.url),
      detail: sanitizeDebugDetail(source.detail),
    })

    if (isFatal || event === 'playback-started') {
      hlsStartupScheduler.release(senderId)
    }
  }

  getDebugLogs() {
    return this.debugLogs.slice()
  }

  getRuntimeDiagnostics() {
    const metrics = app.getAppMetrics().map((metric) => ({
      pid: metric.pid,
      type: metric.type,
      memory: {
        workingSetSize: metric.memory.workingSetSize,
      },
    }))
    const gridRendererProcessIds = Array.from(this.views.values(), (view) =>
      view.getSnapshot().context.view.webContents.getOSProcessId(),
    )

    return createRuntimeDiagnostics({
      appVersion: app.getVersion(),
      metrics,
      gridRendererProcessIds,
      activeViewCount: this.views.size,
      systemMemory: process.getSystemMemoryInfo(),
    })
  }

  getMediaHealthSnapshot(now = Date.now()) {
    const views = Array.from(this.mediaHealth, ([viewId, sample]) => {
      const context = this.views.get(viewId)?.getSnapshot().context
      if (!context || now - sample.sampledAt > MEDIA_HEALTH_FRESH_MS) {
        this.mediaHealth.delete(viewId)
        return undefined
      }
      return {
        viewIdx: context.pos?.spaces[0],
        streamId: context.content?.streamId,
        rendererPid: context.view.webContents.getOSProcessId(),
        fps: sample.presentedFrames === null ? null :
          Math.round(sample.presentedFrames * 1000 / sample.elapsedMs * 10) / 10,
        ...sample,
      }
    }).filter((view) => view !== undefined)
    return {
      windowId: this.win.id,
      cols: this.config.cols,
      rows: this.config.rows,
      activeViews: this.views.size,
      sampledViews: views.length,
      views,
    }
  }

  private findActor(viewId: number) {
    const spotlight = this.spotlightActor
    return this.views.get(viewId) ?? this.probeViews.get(viewId) ??
      (spotlight?.getSnapshot().context.id === viewId ? spotlight : undefined)
  }

  private markProbePlaybackStarted(viewId: number) {
    if (!this.probeViews.has(viewId)) return
    this.probePlaybackStarted.add(viewId)
    this.probePlaybackWaiters.get(viewId)?.()
  }

  private forgetProbe(viewId: number) {
    this.probeViews.delete(viewId)
    this.promotableProbeIds.delete(viewId)
    this.probePlaybackStarted.delete(viewId)
    this.probePlaybackWaiters.delete(viewId)
  }

  private closeProbe(viewId: number) {
    const actor = this.probeViews.get(viewId)
    if (!actor) return
    this.forgetProbe(viewId)
    const view = actor.getSnapshot().context.view
    hlsStartupScheduler.cancel(viewId)
    if (!this.win.isDestroyed()) {
      try { this.win.contentView.removeChildView(view) } catch { /* already detached */ }
    }
    if (!view.webContents.isDestroyed()) {
      view.webContents.close()
    }
  }

  /** Verify playback in a single offscreen view; the caller must promote or close it. */
  async warmStream(stream: StreamData, timeoutMs = 75_000, signal?: AbortSignal): Promise<number | null> {
    if (this.win.isDestroyed() || signal?.aborted) return null
    const content: ViewContent = {
      url: stream.link,
      kind: stream.kind || 'video',
      streamId: stream._id,
    }
    const actor = this.createView(content)
    const viewId = actor.getSnapshot().context.id
    this.probeViews.set(viewId, actor)

    return new Promise<number | null>((resolve) => {
      let settled = false
      const finish = (ready: boolean) => {
        if (settled) return
        settled = true
        clearTimeout(timer)
        subscription.unsubscribe()
        signal?.removeEventListener('abort', onAbort)
        this.probePlaybackWaiters.delete(viewId)
        if (!ready) this.closeProbe(viewId)
        resolve(ready ? viewId : null)
      }
      const onAbort = () => finish(false)
      const checkReady = () => {
        if (actor.getSnapshot().matches({ displaying: 'running' }) &&
            this.probePlaybackStarted.has(viewId)) finish(true)
      }
      const subscription = actor.subscribe((snapshot) => {
        if (snapshot.matches({ displaying: 'error' })) finish(false)
        else checkReady()
      })
      const timer = setTimeout(() => finish(false), timeoutMs)
      this.probePlaybackWaiters.set(viewId, checkReady)
      signal?.addEventListener('abort', onAbort, { once: true })
      actor.send({ type: 'OPTIONS', options: getDisplayOptions(stream) })
      actor.send({
        type: 'DISPLAY',
        content,
        pos: { x: -2, y: -2, width: 1, height: 1, spaces: [] },
      })
    })
  }

  promoteWarmStream(viewId: number) {
    const probe = this.probeViews.get(viewId)
    if (!probe?.getSnapshot().matches({ displaying: 'running' }) ||
        !this.probePlaybackStarted.has(viewId)) return false
    this.promotableProbeIds.add(viewId)
    return true
  }

  discardWarmStream(viewId: number) {
    this.closeProbe(viewId)
  }

  createView(content?: ViewContent) {
    const { win } = this
    assert(win != null, 'Window must be initialized')
    const { backgroundColor } = this.config
    const useHlsMediaPool = isIowaDotHlsURL(content?.url)
    const sessionPool = StreamWindow.nextHlsMediaSessionPool
    if (useHlsMediaPool) {
      StreamWindow.nextHlsMediaSessionPool =
        (StreamWindow.nextHlsMediaSessionPool + 1) %
        HLS_MEDIA_SESSION_POOL_COUNT
    }
    const partition = getMediaSessionPartition(content?.url, sessionPool)
    const view = new WebContentsView({
      webPreferences: {
        preload: path.join(__dirname, 'mediaPreload.js'),
        nodeIntegration: false,
        contextIsolation: true,
        backgroundThrottling: false,
        partition,
      },
    })
    view.setBackgroundColor(backgroundColor)

    const viewId = view.webContents.id
    if (useHlsMediaPool) {
      console.debug('[HLS sessions] assigned media view', {
        viewId,
        partition,
      })
    }
    hlsRequestScheduler.registerRenderer(viewId, (event) =>
      this.recordHlsRequestEvent(viewId, event),
    )
    StreamWindow.registerSender(viewId, this)
    view.webContents.once('destroyed', () => {
      if (this.spotlightActor?.getSnapshot().context.id === viewId) {
        this.finishSpotlight()
      }
      this.mediaHealth.delete(viewId)
      this.forgetProbe(viewId)
      hlsStartupScheduler.cancel(viewId)
      hlsRequestScheduler.unregisterRenderer(viewId)
      StreamWindow.senderToWindow.delete(viewId)
    })

    // Prevent view pages from navigating away from the specified URL.
    view.webContents.on('will-navigate', (ev) => {
      ev.preventDefault()
    })

    const actor = createActor(viewStateMachine, {
      input: {
        id: viewId,
        view,
        win,
      },
    })

    let lastSnapshot: SnapshotFrom<typeof viewStateMachine> | undefined
    actor.subscribe((snapshot) => {
      if (snapshot === lastSnapshot) {
        return
      }
      lastSnapshot = snapshot
      if (this.spotlightSourceId === viewId &&
          this.spotlightPhase === 'preparing' && !this.spotlightActor) {
        if (snapshot.matches({ displaying: 'running' })) {
          this.prepareSpotlightDuplicate(actor)
        } else if (snapshot.matches({ displaying: 'error' })) {
          this.finishSpotlight()
        }
      }
      if (this.views.has(viewId)) this.emitState()
    })

    actor.start()

    return actor
  }

  emitState() {
    const states = Array.from(this.views.values(), (actor) => {
      const { value, context } = actor.getSnapshot()
      return {
        state: value,
        context: {
          id: context.id,
          content: context.content,
          info: context.info,
          pos: context.pos,
        },
      } satisfies ViewState
    })
    this.emit('state', states)
  }

  setViews(viewContentMap: ViewContentMap, streams: StreamList) {
    const { width, height, cols, rows } = this.config
    const spaceWidth = Math.floor(width / cols)
    const spaceHeight = Math.floor(height / rows)
    const { win, views } = this
    const boxes = boxesFromViewContentMap(cols, rows, viewContentMap)
    const remainingBoxes = new Set(boxes)
    const unusedViews = new Set(views.values())
    for (const probeId of Array.from(this.promotableProbeIds)) {
      const warm = this.probeViews.get(probeId)
      if (warm) unusedViews.add(warm)
    }
    const viewsToDisplay = []

    // We try to find the best match for moving / reusing existing views to match the new positions.
    const matchers: Array<
      (
        v: SnapshotFrom<typeof viewStateMachine>,
        content: ViewContent | undefined,
        spaces?: number[],
      ) => boolean
    > = [
      // Keep errored views with the same content so they remain failed instead of being recreated and retried.
      (v, content) =>
        isEqual(v.context.content, content) &&
        v.matches({ displaying: 'error' }),
      // First try to find a loaded view of the same URL in the same space...
      (v, content, spaces) =>
        isEqual(v.context.content, content) &&
        v.matches({ displaying: 'running' }) &&
        intersection(v.context.pos?.spaces, spaces).length > 0,
      // Then try to find a loaded view of the same URL...
      (v, content) =>
        isEqual(v.context.content, content) &&
        v.matches({ displaying: 'running' }),
      // Then try view with the same URL that is still loading...
      (v, content) => isEqual(v.context.content, content),
    ]

    for (const matcher of matchers) {
      for (const box of remainingBoxes) {
        const { content, spaces } = box
        let foundView
        for (const view of unusedViews) {
          const snapshot = view.getSnapshot()
          if (matcher(snapshot, content, spaces)) {
            foundView = view
            break
          }
        }
        if (foundView) {
          viewsToDisplay.push({ box, view: foundView })
          unusedViews.delete(foundView)
          remainingBoxes.delete(box)
          this.forgetProbe(foundView.getSnapshot().context.id)
        }
      }
    }

    for (const box of remainingBoxes) {
      const view = this.createView(box.content)
      viewsToDisplay.push({ box, view })
    }

    const newViews = new Map()
    for (const { box, view } of viewsToDisplay) {
      const { content, x, y, w, h, spaces } = box
      if (!content) {
        continue
      }

      const stream = content.streamId
        ? streams.byId?.get(content.streamId)
        : pickStreamByUrl(streams, content.url)
      if (!stream) {
        continue
      }

      const pos = {
        x: spaceWidth * x,
        y: spaceHeight * y,
        width: spaceWidth * w,
        height: spaceHeight * h,
        spaces,
      }

      const snapshot = view.getSnapshot()
      // If this view is already errored for the same content, keep it as-is to avoid a reload loop.
      if (
        snapshot.matches({ displaying: 'error' }) &&
        isEqual(snapshot.context.content, content)
      ) {
        newViews.set(snapshot.context.id, view)
        continue
      }

      // Send display options first so the view has them during initial navigation.
      view.send({ type: 'OPTIONS', options: getDisplayOptions(stream) })
      view.send({ type: 'DISPLAY', pos, content })
      newViews.set(view.getSnapshot().context.id, view)
    }
    for (const view of unusedViews) {
      const contentView = view.getSnapshot().context.view
      const viewId = contentView.webContents.id
      this.forgetProbe(viewId)
      win.contentView.removeChildView(contentView)
      hlsStartupScheduler.cancel(viewId)
      contentView.webContents.close()
      StreamWindow.senderToWindow.delete(viewId)
    }
    this.views = newViews
    this.promotableProbeIds.clear()
    this.applySpotlight()
    this.emitState()
  }

  setListeningView(viewIdx: number | null) {
    const { views } = this
    for (const view of views.values()) {
      const snapshot = view.getSnapshot()
      if (!snapshot.matches('displaying')) {
        continue
      }
      const { context } = snapshot
      const isSelectedView =
        viewIdx != null
          ? (context.pos?.spaces.includes(viewIdx) ?? false)
          : false
      view.send({ type: isSelectedView ? 'UNMUTE' : 'MUTE' })
    }
  }

  findViewByIdx(viewIdx: number) {
    for (const view of this.views.values()) {
      if (view.getSnapshot().context.pos?.spaces?.includes?.(viewIdx)) {
        return view
      }
    }
  }

  sendViewEvent(viewIdx: number, event: EventFrom<typeof viewStateMachine>) {
    const view = this.findViewByIdx(viewIdx)
    if (view) {
      view.send(event)
    }
  }

  setViewBackgroundListening(viewIdx: number, listening: boolean) {
    this.sendViewEvent(viewIdx, {
      type: listening ? 'BACKGROUND' : 'UNBACKGROUND',
    })
  }

  setViewBlurred(viewIdx: number, blurred: boolean) {
    this.sendViewEvent(viewIdx, { type: blurred ? 'BLUR' : 'UNBLUR' })
  }

  reloadView(viewIdx: number) {
    this.sendViewEvent(viewIdx, { type: 'RELOAD' })
  }

  openDevTools(viewIdx: number, inWebContents: WebContents) {
    this.sendViewEvent(viewIdx, { type: 'DEVTOOLS', inWebContents })
  }

  async refreshAllViewsSequentially(delayMs = 1000) {
    const views = Array.from(this.views.values()).filter((view) =>
      view.getSnapshot().matches('displaying'),
    )
    await this.runRefreshPass(views, 'all', delayMs)
  }

  async refreshErroredViewsSequentially(delayMs = 1000) {
    const erroredViews = Array.from(this.views.values()).filter((view) =>
      view.getSnapshot().matches({ displaying: 'error' }),
    )
    if (erroredViews.length === 0) {
      console.debug('No errored views found to refresh')
      return
    }
    await this.runRefreshPass(erroredViews, 'errored', delayMs)
  }

  private async runRefreshPass(
    views: ViewActor[],
    label: 'all' | 'errored',
    delayMs: number,
  ) {
    if (this.refreshInProgress) {
      console.debug(
        `Skipping ${label}-view refresh; another refresh pass is still running`,
      )
      return this.refreshInProgress
    }

    const safeDelayMs = Math.max(0, delayMs)
    this.refreshInProgress = (async () => {
      console.debug(
        `Starting sequential refresh of ${views.length} ${label} views with ${safeDelayMs}ms delay...`,
      )

      for (let i = 0; i < views.length; i++) {
        const view = views[i]
        const snapshot = view.getSnapshot()
        const { content } = snapshot.context
        console.debug(
          `Refreshing ${label} view ${i + 1}/${views.length}: ${content?.url || 'unknown'}`,
        )
        view.send({ type: 'RELOAD' })

        if (i < views.length - 1) {
          await new Promise((resolve) => setTimeout(resolve, safeDelayMs))
        }
      }

      console.debug(`Finished sequential refresh of ${label} views`)
    })()

    try {
      await this.refreshInProgress
    } finally {
      this.refreshInProgress = null
    }
  }

  onState(state: StreamwallState) {
    this.overlayView.webContents.send('state', state)
    this.syncSpotlightOverlay()
    this.backgroundView.webContents.send('state', state)

    for (const view of this.spotlightActor
      ? [...this.views.values(), this.spotlightActor]
      : this.views.values()) {
      const { content } = view.getSnapshot().context
      if (!content) {
        continue
      }

      const { url, streamId } = content
      const stream = streamId
        ? state.streams.byId?.get(streamId)
        : pickStreamByUrl(state.streams, url)
      if (stream) {
        view.send({
          type: 'OPTIONS',
          options: getDisplayOptions(stream),
        })
      }
    }
  }

  toggleSpotlight(url?: string, streamId?: string) {
    const actor = Array.from(this.views.values()).find((view) => {
      const content = view.getSnapshot().context.content
      if (!content) return false
      return streamId != null
        ? content.streamId === streamId
        : url != null && content.url === url
    })
    if (!actor) return
    const content = actor.getSnapshot().context.content
    if (!content) return
    const token = content.streamId ?? content.url
    this.setSpotlight(this.spotlightToken === token ? null : token)
  }

  private setSpotlight(token: string | null) {
    if (token === this.spotlightToken && this.spotlightPhase !== 'closing') return
    if (token === null) {
      if (this.spotlightPhase !== 'open' || !this.spotlightActor) {
        this.finishSpotlight()
        return
      }
      const source = this.views.get(this.spotlightSourceId ?? -1)
      const destination = source?.getSnapshot().context.pos
      const view = this.spotlightActor.getSnapshot().context.view
      if (!destination || view.webContents.isDestroyed()) {
        this.finishSpotlight()
        return
      }
      this.spotlightPhase = 'closing'
      this.syncSpotlightOverlay()
      this.animateSpotlight(view, view.getBounds(), destination, () =>
        this.finishSpotlight(),
      )
      return
    }

    this.finishSpotlight()
    const source = this.findSpotlightView(token)
    const snapshot = source?.getSnapshot()
    const content = snapshot?.context.content
    if (!source || !snapshot || !content || !snapshot.context.pos ||
        snapshot.matches({ displaying: 'error' })) return

    this.spotlightToken = token
    this.spotlightSourceId = snapshot.context.id
    this.spotlightPhase = 'preparing'
    this.spotlightStartTimer = setTimeout(() => {
      if (this.spotlightToken !== token || this.spotlightPhase !== 'preparing') return
      console.warn('[spotlight] stream timed out while opening', token)
      this.finishSpotlight()
    }, SPOTLIGHT_START_TIMEOUT_MS)
    this.syncSpotlightOverlay()
    this.emit('spotlight-changed', token)
    if (snapshot.matches({ displaying: 'running' })) {
      this.prepareSpotlightDuplicate(source)
    }
  }

  private prepareSpotlightDuplicate(source: ViewActor) {
    if (this.spotlightPhase !== 'preparing' || this.spotlightActor ||
        source.getSnapshot().context.id !== this.spotlightSourceId) return
    const snapshot = source.getSnapshot()
    const content = snapshot.context.content
    const token = this.spotlightToken
    if (!content || !token || !snapshot.matches({ displaying: 'running' })) return
    let actor: ViewActor
    try {
      actor = this.createView(content)
    } catch (error) {
      console.warn('[spotlight] failed to create duplicate view', error)
      this.finishSpotlight()
      return
    }
    this.spotlightActor = actor
    this.spotlightSubscription = actor.subscribe((next) => {
      if (this.spotlightActor !== actor) return
      if (next.matches({ displaying: 'error' })) {
        console.warn('[spotlight] duplicate stream failed to load', token)
        this.finishSpotlight()
      } else if (this.spotlightPhase === 'preparing' &&
          next.matches({ displaying: 'running' })) {
        this.openSpotlight()
      }
    })
    actor.send({ type: 'OPTIONS', options: snapshot.context.options ?? {} })
    actor.send({
      type: 'DISPLAY',
      content,
      pos: { x: -2, y: -2, width: 1, height: 1, spaces: [] },
    })
  }

  private openSpotlight() {
    const actor = this.spotlightActor
    const source = this.views.get(this.spotlightSourceId ?? -1)
    const sourceSnapshot = source?.getSnapshot()
    const from = sourceSnapshot?.context.pos
    if (!actor || !source || !from ||
        !sourceSnapshot.matches({ displaying: 'running' }) ||
        this.win.isDestroyed()) {
      this.finishSpotlight()
      return
    }
    const view = actor.getSnapshot().context.view
    if (view.webContents.isDestroyed()) {
      this.finishSpotlight()
      return
    }
    if (this.spotlightStartTimer) clearTimeout(this.spotlightStartTimer)
    this.spotlightStartTimer = undefined
    this.spotlightSourceWasBlurred = sourceSnapshot.matches(
      'displaying.running.video.blurred',
    )
    this.spotlightPhase = 'open'
    try {
      this.win.contentView.addChildView(
        view,
        this.win.contentView.children.length - 2,
      )
      view.setBounds(from)
      if (!this.spotlightSourceWasBlurred) source.send({ type: 'BLUR' })
      this.syncSpotlightOverlay()
      this.animateSpotlight(view, from, this.spotlightBounds(), () =>
        this.applySpotlight(),
      )
    } catch (error) {
      console.warn('[spotlight] failed to open duplicate view', error)
      this.finishSpotlight()
    }
  }

  private spotlightBounds(): Rectangle {
    const bounds = this.win.getContentBounds()
    const width = Math.max(1, Math.round(bounds.width * 0.8))
    const height = Math.max(1, Math.round(bounds.height * 0.8))
    return {
      x: Math.round((bounds.width - width) / 2),
      y: Math.round((bounds.height - height) / 2),
      width,
      height,
    }
  }

  private animateSpotlight(
    view: WebContentsView,
    from: Rectangle,
    to: Rectangle,
    onDone: () => void,
  ) {
    if (this.spotlightAnimation) clearTimeout(this.spotlightAnimation)
    this.spotlightAnimation = undefined
    const generation = ++this.spotlightAnimationGeneration
    const started = Date.now()
    const step = () => {
      if (generation !== this.spotlightAnimationGeneration ||
          view.webContents.isDestroyed() || this.win.isDestroyed()) return
      const progress = Math.min(1, (Date.now() - started) / SPOTLIGHT_ANIMATION_MS)
      const eased = 1 - Math.pow(1 - progress, 3)
      const value = (start: number, end: number) =>
        Math.round(start + (end - start) * eased)
      try {
        view.setBounds({
          x: value(from.x, to.x),
          y: value(from.y, to.y),
          width: Math.max(1, value(from.width, to.width)),
          height: Math.max(1, value(from.height, to.height)),
        })
      } catch (error) {
        console.warn('[spotlight] failed to animate view', error)
        onDone()
        return
      }
      if (progress < 1) {
        this.spotlightAnimation = setTimeout(step, 16)
      } else {
        this.spotlightAnimation = undefined
        onDone()
      }
    }
    step()
  }

  private finishSpotlight() {
    const token = this.spotlightToken
    if (this.spotlightStartTimer) clearTimeout(this.spotlightStartTimer)
    if (this.spotlightAnimation) clearTimeout(this.spotlightAnimation)
    ++this.spotlightAnimationGeneration
    this.spotlightStartTimer = this.spotlightAnimation = undefined
    this.spotlightSubscription?.unsubscribe()
    this.spotlightSubscription = undefined
    const actor = this.spotlightActor
    const source = this.views.get(this.spotlightSourceId ?? -1)
    const shouldUnblur = !this.spotlightSourceWasBlurred && this.spotlightPhase !== 'preparing'
    this.spotlightActor = null
    this.spotlightSourceId = null
    this.spotlightSourceWasBlurred = false
    this.spotlightToken = null
    this.spotlightPhase = null
    if (actor) {
      const view = actor.getSnapshot().context.view
      hlsStartupScheduler.cancel(view.webContents.id)
      if (!this.win.isDestroyed()) {
        try { this.win.contentView.removeChildView(view) } catch { /* detached */ }
      }
      if (!view.webContents.isDestroyed()) view.webContents.close()
      actor.stop()
    }
    if (shouldUnblur && source &&
        !source.getSnapshot().context.view.webContents.isDestroyed()) {
      source.send({ type: 'UNBLUR' })
    }
    if (token !== null) {
      this.syncSpotlightOverlay()
      this.emit('spotlight-changed', null)
    }
  }

  private syncSpotlightOverlay() {
    if (this.overlayView.webContents.isDestroyed()) return
    const selected = this.findSpotlightView()?.getSnapshot().context.content
    this.overlayView.webContents.send('spotlight', {
      url: selected?.url,
      streamId: selected?.streamId,
      phase: this.spotlightPhase,
    })
  }

  private findSpotlightView(token = this.spotlightToken) {
    if (!token) return undefined
    return Array.from(this.views.values()).find((view) => {
      const content = view.getSnapshot().context.content
      return (
        content?.streamId === token ||
        content?.url === token
      )
    })
  }

  private applySpotlight() {
    if (!this.spotlightToken) return
    const source = this.findSpotlightView()
    if (!source || source.getSnapshot().context.id !== this.spotlightSourceId) {
      this.finishSpotlight()
      return
    }
    if (this.spotlightPhase === 'preparing' && !this.spotlightActor &&
        source.getSnapshot().matches({ displaying: 'running' })) {
      this.prepareSpotlightDuplicate(source)
    }
    if (this.spotlightPhase !== 'open' || this.spotlightAnimation ||
        !this.spotlightActor || this.win.isDestroyed()) return
    const view = this.spotlightActor.getSnapshot().context.view
    if (!view.webContents.isDestroyed()) view.setBounds(this.spotlightBounds())
  }
}
