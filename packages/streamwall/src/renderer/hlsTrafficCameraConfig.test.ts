import assert from 'node:assert/strict'
import test from 'node:test'
import {
  createTrafficCameraHlsConfig,
  isExpiredMediaSegmentError,
  shouldGrantStartupGrace,
  shouldMarkPlaybackProgress,
  TRAFFIC_CAMERA_REQUEST_ADMISSION_FAILURE_MS,
  TRAFFIC_CAMERA_STARTUP_FAILURE_MS,
  TRAFFIC_CAMERA_STARTUP_GRACE_MS,
} from './hlsTrafficCameraConfig'

test('uses the traffic-camera stability profile', () => {
  const config = createTrafficCameraHlsConfig({ random: () => 0.5 })

  assert.equal(config.lowLatencyMode, false)
  assert.equal(config.enableWorker, false)
  assert.equal(config.maxBufferLength, 20)
  assert.equal(config.maxMaxBufferLength, 30)
  assert.equal(config.backBufferLength, 2)
  assert.equal(config.maxBufferSize, 12 * 1024 * 1024)
  assert.equal(config.liveSyncDurationCount, 2)
  assert.equal(config.liveMaxLatencyDurationCount, 3)
  assert.equal(config.manifestLoadPolicy?.default.maxTimeToFirstByteMs, 20_000)
  assert.equal(config.playlistLoadPolicy?.default.maxLoadTimeMs, 30_000)
  assert.equal(config.fragLoadPolicy?.default.maxLoadTimeMs, 60_000)
})

test('does not retry an expired media segment URL', () => {
  const config = createTrafficCameraHlsConfig({ random: () => 0.5 })
  const retry = config.fragLoadPolicy?.default.errorRetry

  assert.ok(retry?.shouldRetry)
  assert.equal(
    retry.shouldRetry(
      retry,
      0,
      false,
      { code: 404, data: undefined, url: 'https://example.test/_seg10.mp4' },
      true,
    ),
    false,
  )
  assert.equal(
    retry.shouldRetry(
      retry,
      0,
      false,
      { code: 408, data: undefined, url: 'https://example.test/_seg10.mp4' },
      false,
    ),
    true,
  )
})

test('recognizes only 404 media-fragment failures as expired segments', () => {
  assert.equal(
    isExpiredMediaSegmentError({
      details: 'fragLoadError',
      responseCode: 404,
    }),
    true,
  )
  assert.equal(
    isExpiredMediaSegmentError({
      details: 'fragLoadError',
      responseCode: 500,
      url: 'https://video2.iowadot.gov/camera/_seg10.mp4',
    }),
    false,
  )
  assert.equal(
    isExpiredMediaSegmentError({
      details: 'levelLoadError',
      responseCode: 404,
      url: 'https://video2.iowadot.gov/camera/stream.m3u8',
    }),
    false,
  )
})

test('retries transient 404 responses and applies bounded jitter', () => {
  const config = createTrafficCameraHlsConfig({ random: () => 0 })
  const retry = config.playlistLoadPolicy?.default.errorRetry

  assert.ok(retry?.shouldRetry)
  assert.equal(
    retry.shouldRetry(
      retry,
      0,
      false,
      { code: 404, data: undefined, url: '' },
      false,
    ),
    true,
  )
  assert.equal(retry.retryDelayMs, 563)
  assert.equal(
    retry.shouldRetry(
      retry,
      0,
      false,
      { code: 403, data: undefined, url: '' },
      false,
    ),
    false,
  )
})

test('allows late ready media a grace period before startup failure', () => {
  assert.equal(TRAFFIC_CAMERA_REQUEST_ADMISSION_FAILURE_MS, 180_000)
  assert.equal(TRAFFIC_CAMERA_STARTUP_FAILURE_MS, 90_000)
  assert.equal(TRAFFIC_CAMERA_STARTUP_GRACE_MS, 5_000)
  assert.equal(shouldGrantStartupGrace(false, 2), true)
  assert.equal(shouldGrantStartupGrace(false, 1), false)
  assert.equal(shouldGrantStartupGrace(true, 2), false)
})

test('playback progress ignores teardown events and media without frame data', () => {
  assert.equal(shouldMarkPlaybackProgress(false, 2), true)
  assert.equal(shouldMarkPlaybackProgress(false, 4), true)
  assert.equal(shouldMarkPlaybackProgress(false, 0), false)
  assert.equal(shouldMarkPlaybackProgress(false, 1), false)
  assert.equal(shouldMarkPlaybackProgress(true, 4), false)
})
