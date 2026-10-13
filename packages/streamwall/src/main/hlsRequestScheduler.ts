import type {
  HlsRequestAcquireResult,
  HlsRequestKind,
  HlsRequestOutcome,
} from 'streamwall-shared'
import { HLS_MEDIA_SESSION_POOL_COUNT } from './hlsMediaSessionPool'

export interface HlsRequestSchedulerOptions {
  maxConcurrentPerHost: number
  maxQueueWaitMs: number | null
}

export interface HlsRequest {
  requestId: number
  rendererId: number
  host: string
  kind: HlsRequestKind
}

export type HlsRequestSchedulerEvent =
  | (HlsRequest & {
      type: 'queued'
      queueDepth: number
      hostQueueDepth: number
    })
  | (HlsRequest & {
      type: 'admitted'
      permitId: number
      queueWaitMs: number
      queueDepth: number
      hostQueueDepth: number
      activeHostRequests: number
    })
  | (HlsRequest & {
      type: 'rejected'
      reason: 'queue-timeout' | 'aborted'
      queueWaitMs: number
      queueDepth: number
      hostQueueDepth: number
      activeHostRequests: number
    })
  | (HlsRequest & {
      type: 'released'
      permitId: number
      outcome: HlsRequestOutcome
      activeMs: number
      activeHostRequests: number
    })

type SchedulerObserver = (event: HlsRequestSchedulerEvent) => void

interface QueuedRequest extends HlsRequest {
  enqueuedAt: number
  resolve: (result: HlsRequestAcquireResult) => void
  rejectionTimer?: ReturnType<typeof setTimeout>
}

interface ActiveRequest extends HlsRequest {
  permitId: number
  admittedAt: number
  observer?: SchedulerObserver
}

export const DEFAULT_HLS_REQUEST_SCHEDULER_OPTIONS: HlsRequestSchedulerOptions =
  {
    // Each media session owns Chromium's per-origin HTTP/1.1 connection pool.
    // Keep a global safety ceiling while allowing all six pools to make progress.
    maxConcurrentPerHost: 6 * HLS_MEDIA_SESSION_POOL_COUNT,
    // hls.js cancels superseded live work itself. A separate five-second queue
    // deadline caused healthy playlist and fragment requests to fail under load.
    maxQueueWaitMs: null,
  }

/**
 * Fair, hard per-origin request scheduler.
 *
 * Requests acquire a permit before the renderer creates its XMLHttpRequest,
 * so queue time is not charged against hls.js's network timeout. Each origin
 * has an independent cap, and queued renderers are served round-robin.
 */
export class HlsRequestScheduler {
  private readonly options: HlsRequestSchedulerOptions
  private readonly rendererObservers = new Map<
    number,
    SchedulerObserver | undefined
  >()
  private readonly queue: QueuedRequest[] = []
  private readonly active = new Map<number, ActiveRequest>()
  private readonly activeByHost = new Map<string, number>()
  private readonly rendererOrderByHost = new Map<string, number[]>()
  private readonly lastAdmittedRendererByHost = new Map<string, number>()
  private nextPermitId = 1

  constructor(
    options: HlsRequestSchedulerOptions = DEFAULT_HLS_REQUEST_SCHEDULER_OPTIONS,
  ) {
    this.options = {
      maxConcurrentPerHost: Math.max(
        1,
        Math.floor(options.maxConcurrentPerHost),
      ),
      maxQueueWaitMs:
        options.maxQueueWaitMs == null
          ? null
          : Math.max(1, options.maxQueueWaitMs),
    }
  }

  registerRenderer(rendererId: number, observer?: SchedulerObserver) {
    this.rendererObservers.set(rendererId, observer)
  }

  isRegisteredRenderer(rendererId: number) {
    return this.rendererObservers.has(rendererId)
  }

  unregisterRenderer(rendererId: number) {
    for (const request of this.queue.slice()) {
      if (request.rendererId === rendererId) {
        this.rejectQueued(request, 'aborted')
      }
    }

    for (const request of Array.from(this.active.values())) {
      if (request.rendererId === rendererId) {
        this.release(rendererId, request.permitId, 'aborted')
      }
    }

    for (const [host, rendererOrder] of this.rendererOrderByHost) {
      const nextOrder = rendererOrder.filter((id) => id !== rendererId)
      if (nextOrder.length === 0) {
        this.rendererOrderByHost.delete(host)
        this.lastAdmittedRendererByHost.delete(host)
      } else {
        this.rendererOrderByHost.set(host, nextOrder)
      }
    }

    this.rendererObservers.delete(rendererId)
  }

  acquire(request: HlsRequest): Promise<HlsRequestAcquireResult> {
    if (!this.rendererObservers.has(request.rendererId)) {
      return Promise.resolve({
        admitted: false,
        reason: 'unauthorized',
        queueWaitMs: 0,
      })
    }

    this.cancel(request.rendererId, request.requestId)

    const rendererOrder = this.rendererOrderByHost.get(request.host) ?? []
    if (!rendererOrder.includes(request.rendererId)) {
      rendererOrder.push(request.rendererId)
      this.rendererOrderByHost.set(request.host, rendererOrder)
    }

    return new Promise((resolve) => {
      const queued: QueuedRequest = {
        ...request,
        enqueuedAt: Date.now(),
        resolve,
      }
      if (this.options.maxQueueWaitMs != null) {
        queued.rejectionTimer = setTimeout(() => {
          this.rejectQueued(queued, 'queue-timeout')
        }, this.options.maxQueueWaitMs)
      }
      this.queue.push(queued)
      this.notify(request.rendererId, {
        ...request,
        type: 'queued',
        queueDepth: this.queue.length,
        hostQueueDepth: this.hostQueueDepth(request.host),
      })
      this.pumpHost(request.host)
    })
  }

  cancel(rendererId: number, requestId: number) {
    const queued = this.queue.find(
      (request) =>
        request.rendererId === rendererId && request.requestId === requestId,
    )
    if (queued) {
      this.rejectQueued(queued, 'aborted')
      return
    }

    const active = Array.from(this.active.values()).find(
      (request) =>
        request.rendererId === rendererId && request.requestId === requestId,
    )
    if (active) {
      this.release(rendererId, active.permitId, 'aborted')
    }
  }

  release(rendererId: number, permitId: number, outcome: HlsRequestOutcome) {
    const request = this.active.get(permitId)
    if (!request || request.rendererId !== rendererId) return false

    this.active.delete(permitId)
    const activeHostRequests = Math.max(
      0,
      (this.activeByHost.get(request.host) ?? 1) - 1,
    )
    if (activeHostRequests === 0) {
      this.activeByHost.delete(request.host)
    } else {
      this.activeByHost.set(request.host, activeHostRequests)
    }

    this.notifyObserver(request.observer, {
      ...request,
      type: 'released',
      outcome,
      activeMs: Date.now() - request.admittedAt,
      activeHostRequests,
    })
    this.pumpHost(request.host)
    return true
  }

  getStats() {
    const queuedByHost: Record<string, number> = {}
    for (const request of this.queue) {
      queuedByHost[request.host] = (queuedByHost[request.host] ?? 0) + 1
    }
    return {
      activeRequests: this.active.size,
      queuedRequests: this.queue.length,
      activeByHost: Object.fromEntries(this.activeByHost),
      queuedByHost,
    }
  }

  private notify(rendererId: number, event: HlsRequestSchedulerEvent) {
    this.notifyObserver(this.rendererObservers.get(rendererId), event)
  }

  private notifyObserver(
    observer: SchedulerObserver | undefined,
    event: HlsRequestSchedulerEvent,
  ) {
    if (!observer) return
    try {
      observer(event)
    } catch (error) {
      console.warn('[HLS requests] scheduler observer failed', error)
    }
  }

  private hostQueueDepth(host: string) {
    return this.queue.reduce(
      (count, request) => count + Number(request.host === host),
      0,
    )
  }

  private findFairRequestIndex(host: string) {
    const rendererIds = this.rendererOrderByHost.get(host) ?? []
    if (rendererIds.length === 0) return -1
    const lastRenderer = this.lastAdmittedRendererByHost.get(host)
    const lastIndex =
      lastRenderer == null ? -1 : rendererIds.indexOf(lastRenderer)
    // Keep playback media moving before servicing less time-sensitive work.
    // Fairness is preserved within each priority class by rotating renderers.
    for (const priority of [0, 1, 2]) {
      for (let offset = 1; offset <= rendererIds.length; offset += 1) {
        const nextRenderer =
          rendererIds[(lastIndex + offset) % rendererIds.length]
        const index = this.queue.findIndex(
          (request) =>
            request.host === host &&
            request.rendererId === nextRenderer &&
            this.requestPriority(request.kind) === priority,
        )
        if (index !== -1) return index
      }
    }
    return -1
  }

  private requestPriority(kind: HlsRequestKind) {
    if (kind === 'fragment' || kind === 'key') return 0
    if (kind === 'playlist' || kind === 'manifest') return 1
    return 2
  }

  private pumpHost(host: string) {
    while (
      (this.activeByHost.get(host) ?? 0) < this.options.maxConcurrentPerHost
    ) {
      const index = this.findFairRequestIndex(host)
      if (index === -1) return

      const request = this.queue.splice(index, 1)[0]
      clearTimeout(request.rejectionTimer)
      const permitId = this.nextPermitId++
      const activeHostRequests = (this.activeByHost.get(host) ?? 0) + 1
      const admission = {
        admitted: true as const,
        permitId,
        queueWaitMs: Date.now() - request.enqueuedAt,
        queueDepth: this.queue.length,
        hostQueueDepth: this.hostQueueDepth(host),
        activeHostRequests,
      }
      const active: ActiveRequest = {
        requestId: request.requestId,
        rendererId: request.rendererId,
        host: request.host,
        kind: request.kind,
        permitId,
        admittedAt: Date.now(),
        observer: this.rendererObservers.get(request.rendererId),
      }
      this.active.set(permitId, active)
      this.activeByHost.set(host, activeHostRequests)
      this.lastAdmittedRendererByHost.set(host, request.rendererId)
      this.notifyObserver(active.observer, {
        ...active,
        type: 'admitted',
        queueWaitMs: admission.queueWaitMs,
        queueDepth: admission.queueDepth,
        hostQueueDepth: admission.hostQueueDepth,
        activeHostRequests,
      })
      request.resolve(admission)
    }
  }

  private rejectQueued(
    request: QueuedRequest,
    reason: 'queue-timeout' | 'aborted',
  ) {
    const index = this.queue.indexOf(request)
    if (index === -1) return false

    this.queue.splice(index, 1)
    clearTimeout(request.rejectionTimer)
    const queueWaitMs = Date.now() - request.enqueuedAt
    this.notify(request.rendererId, {
      requestId: request.requestId,
      rendererId: request.rendererId,
      host: request.host,
      kind: request.kind,
      type: 'rejected',
      reason,
      queueWaitMs,
      queueDepth: this.queue.length,
      hostQueueDepth: this.hostQueueDepth(request.host),
      activeHostRequests: this.activeByHost.get(request.host) ?? 0,
    })
    request.resolve({ admitted: false, reason, queueWaitMs })
    return true
  }
}

export const hlsRequestScheduler = new HlsRequestScheduler()
