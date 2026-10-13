import assert from 'assert'
import { app, BrowserWindow, ipcMain, WebContents, WebContentsView } from 'electron'
import EventEmitter from 'events'
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
  refreshInProgress: Promise<void> | null = null
  debugLogs: StreamwallDebugLogEntry[] = []
  nextDebugLogId = 1
  hlsRequestTelemetry = new Map<string, HlsRequestTelemetryBucket>()
  private spotlightToken: string | null = null
  private spotlightViewId: number | null = null
  static ipcRegistered = false
  static senderToWindow = new Map<number, StreamWindow>()
  static nextHlsMediaSessionPool = 0

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
    win.on('close', () => this.emit('close'))
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
      const view = inst.views.get(ev.sender.id)
      if (view) {
        view.send({ type: 'VIEW_INIT' })
        const { content, options } = view.getSnapshot().context
        return { content, options }
      }
    })

    ipcMain.handle(
      'hls-request-acquire',
      async (ev, value: unknown): Promise<HlsRequestAcquireResult> => {
        const inst = StreamWindow.senderToWindow.get(ev.sender.id)
        if (!inst || !inst.views.has(ev.sender.id)) {
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
      if (!inst || !inst.views.has(ev.sender.id)) return
      if (!Number.isSafeInteger(requestId)) return
      hlsRequestScheduler.cancel(ev.sender.id, requestId as number)
    })

    ipcMain.on('hls-request-release', (ev, value: unknown) => {
      const inst = StreamWindow.senderToWindow.get(ev.sender.id)
      const request = parseHlsReleaseInput(value)
      if (!inst || !inst.views.has(ev.sender.id) || !request) return
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
        ?.views.get(ev.sender.id)
        ?.send?.({ type: 'VIEW_LOADED' })
    })

    ipcMain.on('view-info', (ev, { info }) => {
      StreamWindow.senderToWindow
        .get(ev.sender.id)
        ?.views.get(ev.sender.id)
        ?.send?.({ type: 'VIEW_INFO', info })
    })

    ipcMain.on('view-error', (ev, { error }) => {
      const inst = StreamWindow.senderToWindow.get(ev.sender.id)
      if (!inst) return
      hlsStartupScheduler.release(ev.sender.id)
      inst.recordRendererLog(ev.sender.id, error)
      inst.views.get(ev.sender.id)?.send?.({ type: 'VIEW_ERROR', error })
    })

    ipcMain.on('view-debug-log', (ev, { log }) => {
      StreamWindow.senderToWindow
        .get(ev.sender.id)
        ?.recordRendererLog(ev.sender.id, log)
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

    const context = this.views.get(senderId)?.getSnapshot().context
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
    const view = this.views.get(senderId)
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
      if (
        this.spotlightToken &&
        snapshot.matches({ displaying: 'running' }) &&
        (snapshot.context.content?.streamId === this.spotlightToken ||
          snapshot.context.content?.url === this.spotlightToken)
      ) {
        this.applySpotlight()
      }
      this.emitState()
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
      win.contentView.removeChildView(contentView)
      hlsStartupScheduler.cancel(viewId)
      contentView.webContents.close()
      StreamWindow.senderToWindow.delete(viewId)
    }
    this.views = newViews
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

    for (const view of this.views.values()) {
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
    if (this.spotlightToken === token) return
    if (this.spotlightViewId !== null) {
      const previous = this.views.get(this.spotlightViewId)?.getSnapshot().context
      if (previous?.pos && !previous.view.webContents.isDestroyed()) {
        try {
          previous.view.setBounds(previous.pos)
        } catch (error) {
          console.warn('[spotlight] failed to restore view bounds', error)
        }
      }
      this.spotlightViewId = null
    }
    this.spotlightToken = token
    this.applySpotlight()
    this.syncSpotlightOverlay()
    this.emit('spotlight-changed', this.spotlightToken)
  }

  private syncSpotlightOverlay() {
    if (this.overlayView.webContents.isDestroyed()) return
    const selected = this.findSpotlightView()?.getSnapshot().context.content
    this.overlayView.webContents.send('spotlight', {
      url: selected?.url,
      streamId: selected?.streamId,
    })
  }

  private findSpotlightView() {
    if (!this.spotlightToken) return undefined
    return Array.from(this.views.values()).find((view) => {
      const content = view.getSnapshot().context.content
      return (
        content?.streamId === this.spotlightToken ||
        content?.url === this.spotlightToken
      )
    })
  }

  private applySpotlight() {
    if (!this.spotlightToken) return
    const actor = this.findSpotlightView()
    if (!actor) {
      this.setSpotlight(null)
      return
    }
    const snapshot = actor.getSnapshot()
    if (!snapshot.matches({ displaying: 'running' })) return
    const view = snapshot.context.view
    if (view.webContents.isDestroyed() || this.win.isDestroyed()) return
    const bounds = this.win.getContentBounds()
    const width = Math.max(1, Math.round(bounds.width * 0.8))
    const height = Math.max(1, Math.round(bounds.height * 0.8))
    try {
      // Keep the existing player above other tiles and below the clickable overlay.
      this.win.contentView.addChildView(
        view,
        this.win.contentView.children.length - 2,
      )
      view.setBounds({
        x: Math.round((bounds.width - width) / 2),
        y: Math.round((bounds.height - height) / 2),
        width,
        height,
      })
      this.spotlightViewId = snapshot.context.id
    } catch (error) {
      console.warn('[spotlight] failed to enlarge view', error)
    }
  }
}
