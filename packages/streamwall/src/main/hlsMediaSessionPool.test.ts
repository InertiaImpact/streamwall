import assert from 'node:assert/strict'
import test from 'node:test'
import {
  getMediaSessionPartition,
  HLS_MEDIA_SESSION_POOL_COUNT,
  isIowaDotHlsURL,
} from './hlsMediaSessionPool'

test('recognizes Iowa DOT HLS URLs without matching unrelated hosts', () => {
  assert.equal(
    isIowaDotHlsURL(
      'https://video3.iowadot.gov:8888/cedarrapids/ictv20lb/playlist.m3u8',
    ),
    true,
  )
  assert.equal(
    isIowaDotHlsURL('https://video3.iowadot.gov.example/playlist.m3u8'),
    false,
  )
  assert.equal(isIowaDotHlsURL('https://video3.iowadot.gov/sign-in'), false)
})

test('spreads Iowa DOT HLS views across bounded persistent sessions', () => {
  const url =
    'https://video3.iowadot.gov:8888/cedarrapids/ictv20lb/playlist.m3u8'
  const partitions = Array.from(
    { length: HLS_MEDIA_SESSION_POOL_COUNT * 2 },
    (_, index) => getMediaSessionPartition(url, index),
  )

  assert.equal(new Set(partitions).size, HLS_MEDIA_SESSION_POOL_COUNT)
  assert.equal(partitions[0], partitions[HLS_MEDIA_SESSION_POOL_COUNT])
  assert.equal(
    getMediaSessionPartition('https://example.com/video.m3u8', 3),
    'persist:session',
  )
})
