import assert from 'node:assert/strict'
import test from 'node:test'
import {
  getBufferedAheadSeconds,
  getJitteredRecoveryIntervalMs,
  selectStallRecoveryAction,
} from './hlsStallRecovery'

test('finds buffered media ahead of the current playback position', () => {
  const ranges = {
    length: 2,
    start: (index: number) => [10, 30][index],
    end: (index: number) => [20, 50][index],
  }

  assert.equal(getBufferedAheadSeconds(ranges, 15), 5)
  assert.equal(getBufferedAheadSeconds(ranges, 25), 0)
  assert.equal(getBufferedAheadSeconds(ranges, 40), 10)
})

test('jitters recovery attempts over a bounded five-second range', () => {
  assert.equal(getJitteredRecoveryIntervalMs(() => 0), 12_500)
  assert.equal(getJitteredRecoveryIntervalMs(() => 0.5), 15_000)
  assert.equal(getJitteredRecoveryIntervalMs(() => 1), 17_500)
})

test('uses buffered nudges before disruptive HLS recovery', () => {
  assert.equal(
    selectStallRecoveryAction({
      bufferedAheadSeconds: 12,
      totalStallMs: 50_000,
      hardReloadAttempted: false,
    }),
    'nudge-buffered-media',
  )
  assert.equal(
    selectStallRecoveryAction({
      bufferedAheadSeconds: 0,
      totalStallMs: 20_000,
      hardReloadAttempted: false,
    }),
    'resume-loading',
  )
  assert.equal(
    selectStallRecoveryAction({
      bufferedAheadSeconds: 0,
      totalStallMs: 50_000,
      hardReloadAttempted: false,
    }),
    'restart-loading',
  )
  assert.equal(
    selectStallRecoveryAction({
      bufferedAheadSeconds: 12,
      totalStallMs: 90_000,
      hardReloadAttempted: false,
    }),
    'fail',
  )
})
