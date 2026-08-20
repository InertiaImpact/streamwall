import assert from 'node:assert/strict'
import test from 'node:test'
import {
  DEFAULT_HLS_REQUEST_SCHEDULER_OPTIONS,
  HlsRequestScheduler,
  HlsRequestSchedulerEvent,
  HlsRequestSchedulerOptions,
} from './hlsRequestScheduler'

const createScheduler = (overrides: Partial<HlsRequestSchedulerOptions> = {}) =>
  new HlsRequestScheduler({
    maxConcurrentPerHost: 2,
    maxQueueWaitMs: 1_000,
    ...overrides,
  })

const acquire = (
  scheduler: HlsRequestScheduler,
  requestId: number,
  rendererId: number,
  host = 'video.example',
) =>
  scheduler.acquire({
    requestId,
    rendererId,
    host,
    kind: 'playlist',
  })

test('enforces a hard per-origin active-request limit', async () => {
  const scheduler = createScheduler()
  scheduler.registerRenderer(1)
  scheduler.registerRenderer(2)
  scheduler.registerRenderer(3)

  const first = await acquire(scheduler, 101, 1)
  const second = await acquire(scheduler, 102, 2)
  const third = acquire(scheduler, 103, 3)

  assert.equal(first.admitted, true)
  assert.equal(second.admitted, true)
  assert.deepEqual(scheduler.getStats(), {
    activeRequests: 2,
    queuedRequests: 1,
    activeByHost: { 'video.example': 2 },
    queuedByHost: { 'video.example': 1 },
  })

  if (first.admitted) scheduler.release(1, first.permitId, 'completed')
  assert.equal((await third).admitted, true)
  if (second.admitted) scheduler.release(2, second.permitId, 'completed')
})

test('applies connection limits independently by origin', async () => {
  const scheduler = createScheduler({ maxConcurrentPerHost: 1 })
  scheduler.registerRenderer(1)
  scheduler.registerRenderer(2)

  const first = await acquire(scheduler, 201, 1, 'video2.example')
  const otherHost = await acquire(scheduler, 202, 2, 'video3.example')

  assert.equal(first.admitted, true)
  assert.equal(otherHost.admitted, true)
})

test('serves queued renderers fairly instead of favoring one stream', async () => {
  const scheduler = createScheduler({ maxConcurrentPerHost: 1 })
  scheduler.registerRenderer(1)
  scheduler.registerRenderer(2)
  const events: HlsRequestSchedulerEvent[] = []
  scheduler.registerRenderer(3, (event) => events.push(event))

  const active = await acquire(scheduler, 301, 1)
  const rendererOneAgain = acquire(scheduler, 302, 1)
  const rendererTwo = acquire(scheduler, 303, 2)
  const rendererThree = acquire(scheduler, 304, 3)

  if (active.admitted) scheduler.release(1, active.permitId, 'completed')
  const secondAdmission = await rendererTwo
  assert.equal(secondAdmission.admitted, true)
  if (secondAdmission.admitted) {
    scheduler.release(2, secondAdmission.permitId, 'completed')
  }
  const thirdAdmission = await rendererThree
  assert.equal(thirdAdmission.admitted, true)
  if (thirdAdmission.admitted) {
    scheduler.release(3, thirdAdmission.permitId, 'completed')
  }
  assert.equal((await rendererOneAgain).admitted, true)
  assert.ok(events.some((event) => event.type === 'admitted'))
})

test('rejects stale queued work instead of bypassing the cap', async () => {
  const scheduler = createScheduler({
    maxConcurrentPerHost: 1,
    maxQueueWaitMs: 20,
  })
  const events: HlsRequestSchedulerEvent[] = []
  scheduler.registerRenderer(1)
  scheduler.registerRenderer(2, (event) => events.push(event))

  await acquire(scheduler, 401, 1)
  const rejected = await acquire(scheduler, 402, 2)

  assert.deepEqual(rejected, {
    admitted: false,
    reason: 'queue-timeout',
    queueWaitMs: rejected.queueWaitMs,
  })
  assert.ok(rejected.queueWaitMs >= 10)
  assert.equal(scheduler.getStats().activeRequests, 1)
  assert.ok(
    events.some(
      (event) =>
        event.type === 'rejected' && event.reason === 'queue-timeout',
    ),
  )
})

test('cancels queued and active work when a renderer is destroyed', async () => {
  const scheduler = createScheduler({ maxConcurrentPerHost: 1 })
  scheduler.registerRenderer(1)
  scheduler.registerRenderer(2)

  const active = await acquire(scheduler, 501, 1)
  const queued = acquire(scheduler, 502, 2)
  scheduler.unregisterRenderer(2)
  const cancelled = await queued
  assert.equal(cancelled.admitted, false)
  if (!cancelled.admitted) assert.equal(cancelled.reason, 'aborted')

  scheduler.unregisterRenderer(1)
  assert.equal(scheduler.getStats().activeRequests, 0)
  assert.equal(active.admitted, true)
})

test('production defaults use a hard six-request origin budget', () => {
  assert.deepEqual(DEFAULT_HLS_REQUEST_SCHEDULER_OPTIONS, {
    maxConcurrentPerHost: 6,
    maxQueueWaitMs: 5_000,
  })
})
