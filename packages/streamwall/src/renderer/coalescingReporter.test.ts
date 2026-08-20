import assert from 'node:assert/strict'
import test from 'node:test'
import { CoalescedReport, CoalescingReporter } from './coalescingReporter'

test('reports the first event and summarizes repeats', () => {
  const reports: CoalescedReport<number>[] = []
  const reporter = new CoalescingReporter<number>({
    windowMs: 10_000,
    report: (report) => reports.push(report),
  })

  reporter.add('same-error', 1)
  reporter.add('same-error', 2)
  reporter.add('same-error', 3)
  reporter.flushAll()

  assert.equal(reports.length, 2)
  assert.deepEqual(reports[0], {
    value: 1,
    occurrences: 1,
    suppressed: 0,
    coalescedWindowMs: 0,
  })
  assert.equal(reports[1].value, 3)
  assert.equal(reports[1].occurrences, 3)
  assert.equal(reports[1].suppressed, 2)
})

test('does not emit a redundant summary for a single event', () => {
  const reports: CoalescedReport<string>[] = []
  const reporter = new CoalescingReporter<string>({
    windowMs: 10_000,
    report: (report) => reports.push(report),
  })

  reporter.add('one', 'event')
  reporter.flushAll()

  assert.equal(reports.length, 1)
})
