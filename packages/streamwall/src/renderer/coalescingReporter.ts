export interface CoalescedReport<T> {
  value: T
  occurrences: number
  suppressed: number
  coalescedWindowMs: number
}

export interface CoalescingReporterOptions<T> {
  windowMs: number
  report: (report: CoalescedReport<T>) => void
}

interface PendingReport<T> {
  value: T
  occurrences: number
  startedAt: number
  timer: ReturnType<typeof setTimeout>
}

export class CoalescingReporter<T> {
  private readonly pending = new Map<string, PendingReport<T>>()

  constructor(private readonly options: CoalescingReporterOptions<T>) {}

  add(key: string, value: T) {
    const existing = this.pending.get(key)
    if (existing) {
      existing.value = value
      existing.occurrences += 1
      return
    }

    this.options.report({
      value,
      occurrences: 1,
      suppressed: 0,
      coalescedWindowMs: 0,
    })

    const pending: PendingReport<T> = {
      value,
      occurrences: 1,
      startedAt: Date.now(),
      timer: setTimeout(() => this.flush(key), this.options.windowMs),
    }
    this.pending.set(key, pending)
  }

  flush(key: string) {
    const pending = this.pending.get(key)
    if (!pending) return

    clearTimeout(pending.timer)
    this.pending.delete(key)
    if (pending.occurrences <= 1) return

    this.options.report({
      value: pending.value,
      occurrences: pending.occurrences,
      suppressed: pending.occurrences - 1,
      coalescedWindowMs: Date.now() - pending.startedAt,
    })
  }

  flushAll() {
    for (const key of Array.from(this.pending.keys())) {
      this.flush(key)
    }
  }

  clear() {
    for (const pending of Array.from(this.pending.values())) {
      clearTimeout(pending.timer)
    }
    this.pending.clear()
  }
}
