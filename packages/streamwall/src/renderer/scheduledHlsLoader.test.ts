import assert from 'node:assert/strict'
import test from 'node:test'
import type {
  Loader,
  LoaderCallbacks,
  LoaderConfiguration,
  LoaderContext,
  LoaderStats,
} from 'hls.js'
import type {
  HlsRequestAcquireResult,
  HlsRequestReleaseInput,
} from 'streamwall-shared'
import {
  createScheduledHlsLoader,
  createSingleAttemptLoaderConfig,
  HlsRequestBridge,
} from './scheduledHlsLoader'

const createStats = (): LoaderStats => ({
  aborted: false,
  loaded: 0,
  retry: 0,
  total: 0,
  chunkCount: 0,
  bwEstimate: 0,
  loading: { start: 0, first: 0, end: 0 },
  parsing: { start: 0, end: 0 },
  buffering: { start: 0, first: 0, end: 0 },
})

class FakeLoader implements Loader<LoaderContext> {
  static instances: FakeLoader[] = []
  context: LoaderContext | null = null
  stats = createStats()
  loadConfig: LoaderConfiguration | undefined
  callbacks: LoaderCallbacks<LoaderContext> | undefined

  constructor() {
    FakeLoader.instances.push(this)
  }

  load(
    context: LoaderContext,
    config: LoaderConfiguration,
    callbacks: LoaderCallbacks<LoaderContext>,
  ) {
    this.context = context
    this.loadConfig = config
    this.callbacks = callbacks
  }

  abort() {
    if (this.context) {
      this.callbacks?.onAbort?.(this.stats, this.context, null)
    }
  }

  destroy() {
    this.context = null
  }
}

function deferred<T>() {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((done) => {
    resolve = done
  })
  return { promise, resolve }
}

const loaderConfig: LoaderConfiguration = {
  loadPolicy: {
    maxTimeToFirstByteMs: 20_000,
    maxLoadTimeMs: 30_000,
    timeoutRetry: {
      maxNumRetry: 2,
      retryDelayMs: 100,
      maxRetryDelayMs: 1_000,
    },
    errorRetry: {
      maxNumRetry: 2,
      retryDelayMs: 100,
      maxRetryDelayMs: 1_000,
    },
  },
  timeout: 20_000,
  maxRetry: 2,
  retryDelay: 100,
  maxRetryDelay: 1_000,
}

test('underlying HLS network timers start only after scheduler admission', async () => {
  FakeLoader.instances = []
  const admission = deferred<HlsRequestAcquireResult>()
  const releases: HlsRequestReleaseInput[] = []
  const bridge: HlsRequestBridge = {
    acquire: () => admission.promise,
    cancel: () => undefined,
    release: (request) => releases.push(request),
  }
  const LoaderClass = createScheduledHlsLoader(FakeLoader, () => bridge)
  const loader = new LoaderClass({} as HlsConfig)
  const context: LoaderContext = { url: 'http://video.example/live.m3u8', responseType: 'text' }
  let succeeded = false
  loader.load(context, loaderConfig, {
    onSuccess: () => {
      succeeded = true
    },
    onError: () => undefined,
    onTimeout: () => undefined,
  })

  const delegate = FakeLoader.instances[0]
  assert.equal(delegate.loadConfig, undefined)
  admission.resolve({
    admitted: true,
    permitId: 7,
    queueWaitMs: 500,
    queueDepth: 3,
    hostQueueDepth: 2,
    activeHostRequests: 6,
  })
  await Promise.resolve()
  await Promise.resolve()

  assert.ok(delegate.loadConfig)
  assert.equal(delegate.loadConfig.loadPolicy.timeoutRetry, null)
  assert.equal(delegate.loadConfig.loadPolicy.errorRetry, null)
  delegate.callbacks?.onSuccess(
    { url: context.url, data: '#EXTM3U', code: 200 },
    delegate.stats,
    context,
    null,
  )
  assert.equal(succeeded, true)
  assert.deepEqual(releases, [{ permitId: 7, outcome: 'completed' }])
})

test('bounded scheduler rejection becomes a retryable loader error', async () => {
  FakeLoader.instances = []
  const bridge: HlsRequestBridge = {
    acquire: async () => ({
      admitted: false,
      reason: 'queue-timeout',
      queueWaitMs: 5_000,
    }),
    cancel: () => undefined,
    release: () => undefined,
  }
  const LoaderClass = createScheduledHlsLoader(FakeLoader, () => bridge)
  const loader = new LoaderClass({} as HlsConfig)
  let errorCode: number | undefined
  loader.load(
    { url: 'http://video.example/live.m3u8', responseType: 'text' },
    loaderConfig,
    {
      onSuccess: () => undefined,
      onError: (error) => {
        errorCode = error.code
      },
      onTimeout: () => undefined,
    },
  )
  await Promise.resolve()
  await Promise.resolve()

  assert.equal(FakeLoader.instances[0].loadConfig, undefined)
  assert.equal(errorCode, 0)
})

test('single-attempt delegate config leaves retries to hls.js error control', () => {
  const singleAttempt = createSingleAttemptLoaderConfig(loaderConfig)
  assert.equal(singleAttempt.loadPolicy.timeoutRetry, null)
  assert.equal(singleAttempt.loadPolicy.errorRetry, null)
  assert.equal(singleAttempt.maxRetry, 0)
})
