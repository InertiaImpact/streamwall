export interface HlsStartupAdmission {
  activeStartups: number
  queuedStartups: number
  queueWaitMs: number
}

export interface HlsStartupSchedulerOptions {
  maxConcurrent: number
  minStartIntervalMs: number
  maxHoldMs: number
  onHoldTimeout?: (viewId: number, holdMs: number) => void
}

interface QueuedStart {
  viewId: number
  enqueuedAt: number
  signal: AbortSignal
  resolve: (admission: HlsStartupAdmission) => void
  reject: (error: Error) => void
  handleAbort: () => void
}

export const DEFAULT_HLS_STARTUP_SCHEDULER_OPTIONS: HlsStartupSchedulerOptions =
  {
    // A small amount of concurrency keeps healthy streams moving while slow or
    // unavailable cameras cannot consume the entire startup pipeline.
    maxConcurrent: 6,
    // Admit two player pages per second. The shared request scheduler remains
    // responsible for limiting their actual origin traffic.
    minStartIntervalMs: 500,
    // Release startup-only capacity quickly. Slow network work continues under
    // the steady-state per-origin/global request scheduler after this release.
    maxHoldMs: 3_000,
    onHoldTimeout: (viewId, holdMs) => {
      console.warn('[HLS startup] releasing timed-out queue slot', {
        viewId,
        holdMs,
      })
    },
  }

function cancellationError() {
  return new Error('HLS startup cancelled')
}

export class HlsStartupScheduler {
  private readonly options: HlsStartupSchedulerOptions
  private readonly queue: QueuedStart[] = []
  private readonly active = new Map<number, ReturnType<typeof setTimeout>>()
  private nextStartAt = 0
  private pumpTimer: ReturnType<typeof setTimeout> | undefined

  constructor(
    options: HlsStartupSchedulerOptions = DEFAULT_HLS_STARTUP_SCHEDULER_OPTIONS,
  ) {
    this.options = {
      ...options,
      maxConcurrent: Math.max(1, Math.floor(options.maxConcurrent)),
      minStartIntervalMs: Math.max(0, options.minStartIntervalMs),
      maxHoldMs: Math.max(1, options.maxHoldMs),
    }
  }

  waitForStart(
    viewId: number,
    signal: AbortSignal,
  ): Promise<HlsStartupAdmission> {
    this.cancel(viewId)

    if (signal.aborted) {
      return Promise.reject(cancellationError())
    }

    return new Promise((resolve, reject) => {
      const request: QueuedStart = {
        viewId,
        enqueuedAt: Date.now(),
        signal,
        resolve,
        reject,
        handleAbort: () => {
          const queueIndex = this.queue.indexOf(request)
          if (queueIndex === -1) return
          this.queue.splice(queueIndex, 1)
          signal.removeEventListener('abort', request.handleAbort)
          reject(cancellationError())
          this.pump()
        },
      }

      signal.addEventListener('abort', request.handleAbort, { once: true })
      this.queue.push(request)
      this.pump()
    })
  }

  release(viewId: number) {
    const holdTimer = this.active.get(viewId)
    if (!holdTimer) return false

    clearTimeout(holdTimer)
    this.active.delete(viewId)
    this.pump()
    return true
  }

  cancel(viewId: number) {
    for (let i = this.queue.length - 1; i >= 0; i--) {
      const request = this.queue[i]
      if (request.viewId !== viewId) continue
      this.queue.splice(i, 1)
      request.signal.removeEventListener('abort', request.handleAbort)
      request.reject(cancellationError())
    }
    this.release(viewId)
  }

  getStats() {
    return {
      activeStartups: this.active.size,
      queuedStartups: this.queue.length,
    }
  }

  private pump() {
    if (this.pumpTimer) {
      clearTimeout(this.pumpTimer)
      this.pumpTimer = undefined
    }

    while (
      this.active.size < this.options.maxConcurrent &&
      this.queue.length > 0
    ) {
      const now = Date.now()
      const intervalRemaining = this.nextStartAt - now
      if (intervalRemaining > 0) {
        this.pumpTimer = setTimeout(() => {
          this.pumpTimer = undefined
          this.pump()
        }, intervalRemaining)
        return
      }

      const request = this.queue.shift()
      if (!request) return
      request.signal.removeEventListener('abort', request.handleAbort)

      if (request.signal.aborted) {
        request.reject(cancellationError())
        continue
      }

      const holdTimer = setTimeout(() => {
        this.options.onHoldTimeout?.(request.viewId, this.options.maxHoldMs)
        this.release(request.viewId)
      }, this.options.maxHoldMs)
      this.active.set(request.viewId, holdTimer)
      this.nextStartAt = now + this.options.minStartIntervalMs

      request.resolve({
        activeStartups: this.active.size,
        queuedStartups: this.queue.length,
        queueWaitMs: now - request.enqueuedAt,
      })
    }
  }
}

export const hlsStartupScheduler = new HlsStartupScheduler()
