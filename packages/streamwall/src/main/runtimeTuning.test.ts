import assert from 'node:assert/strict'
import test from 'node:test'
import {
  applyStreamwallRuntimeTuning,
  STREAMWALL_RENDERER_PROCESS_LIMIT,
} from './runtimeTuning'

test('caps Chromium renderer processes for dense camera walls', () => {
  const switches: Array<[string, string | undefined]> = []
  applyStreamwallRuntimeTuning({
    appendSwitch: (name, value) => switches.push([name, value]),
  })

  assert.equal(STREAMWALL_RENDERER_PROCESS_LIMIT, 16)
  assert.deepEqual(switches, [['renderer-process-limit', '16']])
})
