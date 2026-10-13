import assert from 'node:assert/strict'
import test from 'node:test'
import {
  DEFAULT_HLS_STARTUP_SCHEDULER_OPTIONS,
  HlsStartupScheduler,
  HlsStartupSchedulerOptions,
} from './hlsStartupScheduler'

const createScheduler = (overrides: Partial<HlsStartupSchedulerOptions> = {}) =>
  new HlsStartupScheduler({
    maxConcurrent: 2,
    minStartIntervalMs: 0,
    maxHoldMs: 1_000,
    ...overrides,
  })

test('keeps the production startup queue within the live-wall timing budget', () => {
  assert.equal(DEFAULT_HLS_STARTUP_SCHEDULER_OPTIONS.maxConcurrent, 6)
  assert.equal(DEFAULT_HLS_STARTUP_SCHEDULER_OPTIONS.minStartIntervalMs, 500)
  assert.equal(DEFAULT_HLS_STARTUP_SCHEDULER_OPTIONS.maxHoldMs, 3_000)
})

test('limits active startups and admits queued work in FIFO order', async () => {
  const scheduler = createScheduler()
  const signals = [1, 2, 3].map(() => new AbortController())

  const first = scheduler.waitForStart(1, signals[0].signal)
  const second = scheduler.waitForStart(2, signals[1].signal)
  const third = scheduler.waitForStart(3, signals[2].signal)

  await Promise.all([first, second])
  assert.deepEqual(scheduler.getStats(), {
    activeStartups: 2,
    queuedStartups: 1,
  })

  scheduler.release(1)
  await third
  assert.deepEqual(scheduler.getStats(), {
    activeStartups: 2,
    queuedStartups: 0,
  })

  scheduler.release(2)
  scheduler.release(3)
})

test('removes an aborted startup from the queue', async () => {
  const scheduler = createScheduler({ maxConcurrent: 1 })
  const activeController = new AbortController()
  const queuedController = new AbortController()

  await scheduler.waitForStart(1, activeController.signal)
  const queued = scheduler.waitForStart(2, queuedController.signal)
  queuedController.abort()

  await assert.rejects(queued, /cancelled/)
  assert.deepEqual(scheduler.getStats(), {
    activeStartups: 1,
    queuedStartups: 0,
  })

  scheduler.release(1)
})

test('safety timeout releases a slot held by a stalled startup', async () => {
  const scheduler = createScheduler({
    maxConcurrent: 1,
    maxHoldMs: 10,
    onHoldTimeout: () => undefined,
  })
  const firstController = new AbortController()
  const secondController = new AbortController()

  await scheduler.waitForStart(1, firstController.signal)
  const secondAdmission = await scheduler.waitForStart(
    2,
    secondController.signal,
  )

  assert.ok(secondAdmission.queueWaitMs >= 5)
  assert.deepEqual(scheduler.getStats(), {
    activeStartups: 1,
    queuedStartups: 0,
  })
  scheduler.release(2)
})
