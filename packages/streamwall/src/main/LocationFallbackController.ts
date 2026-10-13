import type { StreamData, StreamList, ViewState } from 'streamwall-shared'
import type StreamWindow from './StreamWindow'

const DESIRED_ATTEMPTS = 3
const START_DEADLINE_MS = 120_000
const FAILED_CANDIDATE_COOLDOWN_MS = 5 * 60_000
const MAX_BACKGROUND_PROBES = 2
let activeBackgroundProbes = 0

type Phase = 'loading' | 'running' | 'error' | 'other'

interface RecoveryGroup {
  key: string
  spaces: number[]
  desiredId: string
  effectiveId: string
  attempts: number
  lastTransition: string
  retryTimer?: NodeJS.Timeout
  deadlineTimer?: NodeJS.Timeout
  healthyTimer?: NodeJS.Timeout
  probeTimer?: NodeJS.Timeout
  probeAbort?: AbortController
  probeFailures: number
}

function phaseOf(view: ViewState): Phase {
  const displaying = typeof view.state === 'object' ? view.state.displaying : undefined
  if (displaying === 'error') return 'error'
  if (typeof displaying === 'object' && 'running' in displaying) return 'running'
  if (typeof displaying === 'object' && 'loading' in displaying) return 'loading'
  return 'other'
}

function coordinates(stream: StreamData) {
  const { latitude, longitude } = stream
  if (
    typeof latitude !== 'number' || typeof longitude !== 'number' ||
    !Number.isFinite(latitude) || !Number.isFinite(longitude) ||
    latitude < -90 || latitude > 90 || longitude < -180 || longitude > 180
  ) return null
  return { latitude, longitude }
}

function distanceKm(a: StreamData, b: StreamData) {
  const first = coordinates(a)
  const second = coordinates(b)
  if (!first || !second) return Number.POSITIVE_INFINITY
  const radians = Math.PI / 180
  const dLat = (second.latitude - first.latitude) * radians
  const dLon = (second.longitude - first.longitude) * radians
  const h = Math.sin(dLat / 2) ** 2 +
    Math.cos(first.latitude * radians) * Math.cos(second.latitude * radians) *
    Math.sin(dLon / 2) ** 2
  return 6371 * 2 * Math.asin(Math.min(1, Math.sqrt(h)))
}

function eligible(stream: StreamData) {
  return (stream.kind === 'video' || stream.kind === 'hdhomerun') &&
    /^https?:\/\//i.test(stream.link) && coordinates(stream) !== null
}

/** Keeps the saved grid assignments separate from temporary nearby cameras. */
export default class LocationFallbackController {
  private desiredIds: Array<string | undefined> = []
  private streams: StreamList = [] as unknown as StreamList
  private groups = new Map<string, RecoveryGroup>()
  private effectiveBySpace = new Map<number, string>()
  private failedCandidates = new Map<string, number>()

  constructor(
    private readonly gridId: string,
    private readonly window: StreamWindow,
    private readonly onChange: () => void,
  ) {}

  effectiveId(space: number, desiredId: string) {
    return this.effectiveBySpace.get(space) ?? desiredId
  }

  originalId(space: number) {
    return this.effectiveBySpace.has(space) ? this.desiredIds[space] : undefined
  }

  sync(desiredIds: Array<string | undefined>, streams: StreamList) {
    this.desiredIds = desiredIds
    this.streams = streams
    for (const group of Array.from(this.groups.values())) {
      if (
        group.spaces.some((space) => desiredIds[space] !== group.desiredId) ||
        !streams.byId?.has(group.desiredId) ||
        !streams.byId?.has(group.effectiveId)
      ) this.removeGroup(group)
    }
  }

  observe(views: ViewState[]) {
    for (const view of views) {
      const spaces = view.context.pos?.spaces
      const streamId = view.context.content?.streamId
      if (!spaces?.length || !streamId) continue
      const desiredId = this.desiredIds[spaces[0]]
      if (!desiredId || spaces.some((space) => this.desiredIds[space] !== desiredId)) continue
      const desired = this.streams.byId?.get(desiredId)
      if (!desired || !eligible(desired)) continue

      const key = `${desiredId}:${spaces.join(',')}`
      let group = this.groups.get(key)
      if (!group) {
        group = {
          key, spaces: [...spaces], desiredId, effectiveId: desiredId,
          attempts: 0, lastTransition: '', probeFailures: 0,
        }
        this.groups.set(key, group)
      }
      if (streamId !== group.effectiveId) continue

      const phase = phaseOf(view)
      const transition = `${view.context.id}:${phase}`
      if (transition === group.lastTransition) continue
      group.lastTransition = transition
      if (phase === 'loading') this.startDeadline(group, transition)
      else if (phase === 'running') this.onRunning(group)
      else if (phase === 'error') this.onFailure(group)
    }
  }

  private clearTimers(group: RecoveryGroup) {
    if (group.retryTimer) clearTimeout(group.retryTimer)
    if (group.deadlineTimer) clearTimeout(group.deadlineTimer)
    if (group.healthyTimer) clearTimeout(group.healthyTimer)
    if (group.probeTimer) clearTimeout(group.probeTimer)
    group.retryTimer = group.deadlineTimer = group.healthyTimer = group.probeTimer = undefined
  }

  private removeGroup(group: RecoveryGroup) {
    this.clearTimers(group)
    group.probeAbort?.abort()
    for (const space of group.spaces) this.effectiveBySpace.delete(space)
    this.groups.delete(group.key)
  }

  private startDeadline(group: RecoveryGroup, transition: string) {
    if (group.retryTimer) clearTimeout(group.retryTimer)
    group.retryTimer = undefined
    if (group.deadlineTimer) clearTimeout(group.deadlineTimer)
    group.deadlineTimer = setTimeout(() => {
      if (this.groups.get(group.key) !== group || group.lastTransition !== transition) return
      console.warn('[fallback] startup deadline exceeded', this.gridId, group.desiredId)
      this.onFailure(group)
    }, START_DEADLINE_MS)
  }

  private onRunning(group: RecoveryGroup) {
    if (group.deadlineTimer) clearTimeout(group.deadlineTimer)
    group.deadlineTimer = undefined
    if (group.effectiveId === group.desiredId) {
      if (group.healthyTimer) clearTimeout(group.healthyTimer)
      group.healthyTimer = setTimeout(() => {
        if (this.groups.get(group.key) === group && group.effectiveId === group.desiredId) {
          this.removeGroup(group)
        }
      }, 30_000)
    } else if (!group.probeTimer && !group.probeAbort) {
      this.scheduleProbe(group, 30_000)
    }
  }

  private onFailure(group: RecoveryGroup) {
    this.clearTimers(group)
    group.probeAbort?.abort()
    if (group.effectiveId === group.desiredId) {
      group.attempts += 1
      if (group.attempts < DESIRED_ATTEMPTS) {
        const delay = group.attempts === 1 ? 2_000 : 5_000
        group.retryTimer = setTimeout(() => {
          if (this.groups.get(group.key) === group) {
            group.lastTransition = `retry:${Date.now()}`
            this.startDeadline(group, group.lastTransition)
            this.window.reloadView(group.spaces[0])
          }
        }, delay)
        return
      }
      console.warn('[fallback] desired stream failed three times', this.gridId, group.desiredId)
    } else {
      this.failedCandidates.set(group.effectiveId, Date.now() + FAILED_CANDIDATE_COOLDOWN_MS)
      console.warn('[fallback] candidate failed', this.gridId, group.effectiveId)
    }
    this.chooseCandidate(group)
  }

  private chooseCandidate(group: RecoveryGroup) {
    const original = this.streams.byId?.get(group.desiredId)
    if (!original) return
    const occupied = new Set(this.desiredIds.filter((id): id is string => !!id))
    for (const id of Array.from(this.effectiveBySpace.values())) occupied.add(id)
    occupied.delete(group.effectiveId)
    occupied.add(group.desiredId)
    const occupiedLinks = new Set(Array.from(occupied, (id) => this.streams.byId?.get(id)?.link))
    const now = Date.now()
    const candidate = this.streams
      .filter((stream) => eligible(stream) &&
        !occupied.has(stream._id) &&
        !occupiedLinks.has(stream.link) && stream.link !== original.link &&
        (this.failedCandidates.get(stream._id) ?? 0) <= now &&
        distanceKm(original, stream) > 0.001)
      .sort((a, b) => distanceKm(original, a) - distanceKm(original, b))[0]

    if (!candidate) {
      const wasBackup = group.effectiveId !== group.desiredId
      for (const space of group.spaces) this.effectiveBySpace.delete(space)
      group.effectiveId = group.desiredId
      group.attempts = 0
      if (wasBackup) {
        group.lastTransition = ''
        this.onChange()
      }
      group.retryTimer = setTimeout(() => {
        if (this.groups.get(group.key) === group) {
          group.lastTransition = `retry:${Date.now()}`
          this.startDeadline(group, group.lastTransition)
          this.window.reloadView(group.spaces[0])
        }
      }, 60_000)
      return
    }

    group.effectiveId = candidate._id
    group.lastTransition = ''
    for (const space of group.spaces) this.effectiveBySpace.set(space, candidate._id)
    console.warn('[fallback] using nearby stream', this.gridId, group.desiredId,
      candidate._id, Math.round(distanceKm(original, candidate) * 10) / 10, 'km')
    this.onChange()
  }

  private scheduleProbe(group: RecoveryGroup, delayMs: number) {
    group.probeTimer = setTimeout(() => {
      group.probeTimer = undefined
      void this.probeOriginal(group)
    }, delayMs)
  }

  private async probeOriginal(group: RecoveryGroup) {
    if (this.groups.get(group.key) !== group || group.effectiveId === group.desiredId) return
    if (activeBackgroundProbes >= MAX_BACKGROUND_PROBES) {
      this.scheduleProbe(group, 15_000)
      return
    }
    const original = this.streams.byId?.get(group.desiredId)
    if (!original) return
    const backupId = group.effectiveId
    const abort = new AbortController()
    group.probeAbort = abort
    activeBackgroundProbes += 1
    let warmId: number | null = null
    try {
      warmId = await this.window.warmStream(original, 75_000, abort.signal)
      if (
        warmId != null && !abort.signal.aborted &&
        this.groups.get(group.key) === group && group.effectiveId === backupId &&
        this.window.promoteWarmStream(warmId)
      ) {
        group.effectiveId = group.desiredId
        group.attempts = 0
        group.lastTransition = ''
        for (const space of group.spaces) this.effectiveBySpace.delete(space)
        console.log('[fallback] original stream restored', this.gridId, group.desiredId)
        this.onChange()
        warmId = null
      }
    } catch (error) {
      console.warn('[fallback] background probe failed', this.gridId, group.desiredId, error)
    } finally {
      if (warmId != null) this.window.discardWarmStream(warmId)
      activeBackgroundProbes -= 1
      if (group.probeAbort === abort) group.probeAbort = undefined
      if (this.groups.get(group.key) === group && group.effectiveId === backupId) {
        group.probeFailures += 1
        this.scheduleProbe(group, Math.min(5 * 60_000, 30_000 * 2 ** group.probeFailures))
      }
    }
  }
}
