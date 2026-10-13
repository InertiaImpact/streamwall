import assert from 'node:assert/strict'
import test from 'node:test'
import {
  canCopyHdHomeRunVideo,
  HdHomeRunBridge,
  isHdHomeRunURL,
} from './hdHomeRun'

test('recognizes HDHomeRun live channel URLs without taking over other streams', () => {
  assert.equal(isHdHomeRunURL('http://192.0.2.10:5004/auto/v2.1'), true)
  assert.equal(isHdHomeRunURL('http://192.0.2.10:5004/auto/ch7'), true)
  assert.equal(
    isHdHomeRunURL('http://192.0.2.10:5004/auto/v2.1?transcode=mobile'),
    true,
  )
  assert.equal(isHdHomeRunURL('https://example.com/live/playlist.m3u8'), false)
  assert.equal(isHdHomeRunURL('http://example.com:5004/other/v2.1'), false)
})

test('copies H.264 channels and transcodes MPEG-2 channels', () => {
  assert.equal(
    canCopyHdHomeRunVideo('Stream #0:0[0x31]: Video: h264 (High)'),
    true,
  )
  assert.equal(
    canCopyHdHomeRunVideo('Stream #0:0[0x31]: Video: mpeg2video (Main)'),
    false,
  )
})

const liveURL = process.env.STREAMWALL_HDHOMERUN_TEST_URL
test(
  'shares one live channel and serves its HLS playlist',
  {
    skip: !liveURL,
    timeout: 45_000,
  },
  async () => {
    if (!liveURL) return
    const bridge = new HdHomeRunBridge(
      process.env.STREAMWALL_FFMPEG_PATH ?? 'ffmpeg',
    )
    try {
      const first = await bridge.acquire(liveURL, 1)
      const second = await bridge.acquire(liveURL, 2)
      assert.equal(first, second)
      const response = await fetch(first)
      assert.equal(response.status, 200)
      const playlist = await response.text()
      assert.match(playlist, /#EXTM3U/)
      const segmentName = playlist.match(/seg\d+\.ts/)?.[0]
      assert.ok(segmentName)
      const segment = await fetch(new URL(segmentName, first))
      assert.equal(segment.status, 200)
      assert.ok((await segment.arrayBuffer()).byteLength > 0)
      bridge.release(1)
      assert.equal((await fetch(second)).status, 200)
      bridge.release(2)
      assert.equal((await fetch(second)).status, 404)
    } finally {
      bridge.close()
    }
  },
)
