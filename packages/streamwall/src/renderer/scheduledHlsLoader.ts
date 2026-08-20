import Hls from 'hls.js'
import type {
  HlsConfig,
  Loader,
  LoaderCallbacks,
  LoaderConfiguration,
  LoaderContext,
  LoaderStats,
} from 'hls.js'
import type {
  HlsRequestAcquireInput,
  HlsRequestAcquireResult,
  HlsRequestKind,
  HlsRequestReleaseInput,
} from 'streamwall-shared'

export interface HlsRequestBridge {
  acquire(request: HlsRequestAcquireInput): Promise<HlsRequestAcquireResult>
  cancel(requestId: number): void
  release(request: HlsRequestReleaseInput): void
}

declare global {
  interface Window {
    streamwallHlsRequests: HlsRequestBridge
  }
}

type LoaderConstructor = new (config: HlsConfig) => Loader<LoaderContext>

let nextRequestId = 1
let reportedFirstAdmission = false

export function classifyHlsRequest(context: LoaderContext): HlsRequestKind {
  if ('frag' in context) return 'fragment'
  if ('keyInfo' in context) return 'key'
  const contextType = (context as LoaderContext & { type?: unknown }).type
  if (contextType === 'manifest') return 'manifest'
  if (
    contextType === 'level' ||
    contextType === 'audioTrack' ||
    contextType === 'subtitleTrack'
  ) {
    return 'playlist'
  }
  return 'other'
}

export function createSingleAttemptLoaderConfig(
  config: LoaderConfiguration,
): LoaderConfiguration {
  return {
    ...config,
    loadPolicy: {
      ...config.loadPolicy,
      timeoutRetry: null,
      errorRetry: null,
    },
    maxRetry: 0,
    retryDelay: 0,
    maxRetryDelay: 0,
  }
}

export function createScheduledHlsLoader(
  BaseLoader: LoaderConstructor,
  getBridge: () => HlsRequestBridge = () => window.streamwallHlsRequests,
) {
  return class ScheduledHlsLoader implements Loader<LoaderContext> {
    context: LoaderContext | null = null
    stats: LoaderStats

    private readonly delegate: Loader<LoaderContext>
    private callbacks: LoaderCallbacks<LoaderContext> | undefined
    private requestId: number | undefined
    private permitId: number | undefined
    private aborted = false
    private destroyed = false

    constructor(config: HlsConfig) {
      this.delegate = new BaseLoader(config)
      this.stats = this.delegate.stats
    }

    load(
      context: LoaderContext,
      config: LoaderConfiguration,
      callbacks: LoaderCallbacks<LoaderContext>,
    ) {
      if (this.context) throw new Error('Loader can only be used once.')
      this.context = context
      this.callbacks = callbacks
      this.requestId = nextRequestId++
      void this.acquireAndLoad(context, config, callbacks)
    }

    abort() {
      if (this.aborted || this.destroyed) return
      this.aborted = true
      this.stats.aborted = true

      if (this.permitId != null) {
        this.delegate.abort()
        this.releasePermit('aborted')
      } else if (this.requestId != null) {
        this.getBridgeSafely()?.cancel(this.requestId)
        if (this.context) {
          this.callbacks?.onAbort?.(this.stats, this.context, null)
        }
      }
    }

    destroy() {
      if (this.destroyed) return
      this.destroyed = true
      if (this.permitId != null) {
        this.releasePermit('aborted')
      } else if (this.requestId != null && !this.aborted) {
        this.getBridgeSafely()?.cancel(this.requestId)
      }
      this.delegate.destroy()
      this.callbacks = undefined
      this.context = null
    }

    getCacheAge() {
      return this.delegate.getCacheAge?.() ?? null
    }

    getResponseHeader(name: string) {
      return this.delegate.getResponseHeader?.(name) ?? null
    }

    private getBridgeSafely() {
      try {
        return getBridge()
      } catch {
        return undefined
      }
    }

    private async acquireAndLoad(
      context: LoaderContext,
      config: LoaderConfiguration,
      callbacks: LoaderCallbacks<LoaderContext>,
    ) {
      const bridge = this.getBridgeSafely()
      if (!bridge || this.requestId == null) {
        this.failAdmission('HLS request scheduler is unavailable')
        return
      }

      let admission: HlsRequestAcquireResult
      try {
        admission = await bridge.acquire({
          requestId: this.requestId,
          url: context.url,
          kind: classifyHlsRequest(context),
        })
      } catch (error) {
        this.failAdmission(
          `HLS request scheduler failed: ${
            error instanceof Error ? error.message : String(error)
          }`,
        )
        return
      }

      if (this.aborted || this.destroyed) {
        if (admission.admitted) {
          bridge.release({
            permitId: admission.permitId,
            outcome: 'aborted',
          })
        }
        return
      }

      if (!admission.admitted) {
        this.failAdmission(`HLS request rejected: ${admission.reason}`)
        return
      }

      this.permitId = admission.permitId
      if (!reportedFirstAdmission && typeof window !== 'undefined') {
        reportedFirstAdmission = true
        window.postMessage({ type: 'streamwall:hls-request-admitted' }, '*')
      }
      const wrappedCallbacks: LoaderCallbacks<LoaderContext> = {
        onSuccess: (response, stats, loadedContext, networkDetails) => {
          this.releasePermit('completed')
          callbacks.onSuccess(response, stats, loadedContext, networkDetails)
        },
        onError: (error, loadedContext, networkDetails, stats) => {
          this.releasePermit('error')
          callbacks.onError(error, loadedContext, networkDetails, stats)
        },
        onTimeout: (stats, loadedContext, networkDetails) => {
          this.releasePermit('timeout')
          callbacks.onTimeout(stats, loadedContext, networkDetails)
        },
        onAbort: (stats, loadedContext, networkDetails) => {
          this.releasePermit('aborted')
          callbacks.onAbort?.(stats, loadedContext, networkDetails)
        },
        onProgress: callbacks.onProgress,
      }
      this.delegate.load(
        context,
        createSingleAttemptLoaderConfig(config),
        wrappedCallbacks,
      )
    }

    private failAdmission(message: string) {
      if (this.aborted || this.destroyed || !this.callbacks || !this.context) {
        return
      }
      this.callbacks.onError(
        { code: 0, text: message },
        this.context,
        null,
        this.stats,
      )
    }

    private releasePermit(outcome: HlsRequestReleaseInput['outcome']) {
      const permitId = this.permitId
      if (permitId == null) return
      this.permitId = undefined
      this.getBridgeSafely()?.release({ permitId, outcome })
    }
  }
}

export const ScheduledHlsLoader = createScheduledHlsLoader(
  Hls.DefaultConfig.loader,
)
