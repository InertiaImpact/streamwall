import TOML from '@iarna/toml'
import { Repeater } from '@repeaterjs/repeater'
import { watch } from 'chokidar'
import { EventEmitter, once } from 'events'
import { promises as fsPromises } from 'fs'
import https from 'https'
import { isArray } from 'lodash-es'
import fetch from 'node-fetch'
import { promisify } from 'util'
import {
  StreamData,
  StreamDataContent,
  StreamList,
} from '../../../streamwall-shared/src/types'

const sleep = promisify(setTimeout)

type DataSource = AsyncGenerator<StreamDataContent[]>
type HlsProbeStatus = 'live' | 'stale' | 'offline' | 'unknown'

export async function* pollDataURL(url: string, intervalSecs: number) {
  const refreshInterval = intervalSecs * 1000
  let lastData = []
  while (true) {
    let data: StreamDataContent[] = []
    try {
      const resp = await fetch(url)
      data = (await resp.json()) as StreamDataContent[]
    } catch (err) {
      console.warn('error loading stream data', err)
    }

    // If the endpoint errors or returns an empty dataset, keep the cached data.
    if (!data.length && lastData.length) {
      console.warn('using cached stream data')
    } else {
      yield data
      lastData = data
    }

    await sleep(refreshInterval)
  }
}

export async function* watchDataFile(path: string): DataSource {
  const watcher = watch(path)
  while (true) {
    let data
    try {
      const text = await fsPromises.readFile(path)
      data = TOML.parse(text.toString())
    } catch (err) {
      console.warn('error reading data file', err)
    }
    if (data && isArray(data.streams)) {
      // TODO: type validate with Zod
      yield data.streams as unknown as StreamList
    } else {
      yield []
    }
    await once(watcher, 'change')
  }
}

export async function* markDataSource(dataSource: DataSource, name: string) {
  for await (const streamList of dataSource) {
    for (const s of streamList) {
      s._dataSource = name
    }
    yield streamList
  }
}

export async function* combineDataSources(
  dataSources: DataSource[],
  idGen: StreamIDGenerator,
) {
  for await (const streamLists of Repeater.latest(dataSources)) {
    const merged: StreamData[] = []
    for (const list of streamLists) {
      for (const data of list) {
        merged.push({ ...(data as StreamData) })
      }
    }

    const streams = idGen.process(merged) as StreamList

    // Retain indexes to speed up lookups
    const byURL = new Map<string, StreamData[]>()
    const byId = new Map<string, StreamData>()
    for (const s of streams) {
      if (s._id) {
        byId.set(s._id, s)
      }
      const arr = byURL.get(s.link) ?? []
      arr.push(s)
      byURL.set(s.link, arr)
    }
    streams.byURL = byURL
    streams.byId = byId
    yield streams
  }
}

// --- HLS liveness probing (for Iowa DOT cameras) ---
const HLS_PROBE_MIN_INTERVAL_MS = 60 * 1000 // 1 minute between probes per URL
const HLS_STALE_THRESHOLD_MS = 45 * 1000 // sequence unchanged for 45s => stale
const HLS_FETCH_TIMEOUT_MS = 20000
const hlsProbeCache = new Map<
  string,
  { checkedAt: number; status: HlsProbeStatus; seq?: number }
>()

const insecureAgent = new https.Agent({ rejectUnauthorized: false })

function isIowaHls(stream: StreamData) {
  const src = (stream.source ?? '').toLowerCase()
  const link = (stream.link ?? '').toLowerCase()
  return (
    (src.includes('iowa') && src.includes('dot')) ||
    link.includes('iowadot') ||
    link.includes('iowa.gov')
  )
}

async function fetchWithTimeout(url: string, agent?: https.Agent) {
  const controller = new AbortController()
  const timeout = setTimeout(() => controller.abort(), HLS_FETCH_TIMEOUT_MS)
  try {
    const resp = await fetch(url, { signal: controller.signal, redirect: 'follow', agent })
    if (!resp.ok) {
      throw new Error(`playlist fetch failed: ${resp.status} ${resp.statusText}`)
    }
    return await resp.text()
  } finally {
    clearTimeout(timeout)
  }
}

function isAbortError(err: unknown) {
  const message = err instanceof Error ? err.message : String(err)
  return message.toLowerCase().includes('aborted') || (err as any)?.name === 'AbortError'
}

async function fetchPlaylist(url: string, agent?: https.Agent) {
  const manifest = await fetchWithTimeout(url, agent)
  // If this is a master playlist, follow the first chunklist variant for a real media sequence.
  const match = manifest.match(/^(.*chunklist[^\n]+)/m)
  if (match) {
    const childUrl = new URL(match[1].trim(), url).toString()
    try {
      return await fetchWithTimeout(childUrl, agent)
    } catch (err) {
      // Fall back to master if child fetch fails.
      console.warn('[hls-probe] failed child playlist fetch for', childUrl, '-', err)
    }
  }
  return manifest
}

async function fetchPlaylistWithRetry(url: string, agent?: https.Agent) {
  let attempt = 0
  while (attempt < 2) {
    try {
      return await fetchPlaylist(url, agent)
    } catch (err) {
      attempt += 1
      if (!isAbortError(err) || attempt >= 2) {
        throw err
      }
      console.warn('[hls-probe] retry after abort for', url)
    }
  }
  throw new Error('unreachable')
}

function parseMediaSequence(manifest: string) {
  const match = manifest.match(/#EXT-X-MEDIA-SEQUENCE:(\d+)/)
  return match ? Number(match[1]) : undefined
}

async function probeSingleIowa(stream: StreamData, now: number): Promise<boolean> {
  let changed = false
  const cached = hlsProbeCache.get(stream.link)
  const applyCachedStatus = (cachedStatus: HlsProbeStatus) => {
    const nextStatus =
      cachedStatus === 'unknown'
        ? stream.status
        : cachedStatus === 'offline'
          ? 'Offline'
          : cachedStatus === 'stale'
            ? 'Stale'
            : 'Live'
    const nextIsLive = cachedStatus === 'live'
    if (stream.status !== nextStatus || stream.isLive !== nextIsLive) {
      stream.status = nextStatus
      stream.isLive = nextIsLive
      changed = true
    }
  }

  if (cached && now - cached.checkedAt < HLS_PROBE_MIN_INTERVAL_MS) {
    applyCachedStatus(cached.status)
    return changed
  }

  let status: HlsProbeStatus = 'unknown'
  let seq: number | undefined
  try {
    const manifest = await fetchPlaylistWithRetry(stream.link, insecureAgent)
    seq = parseMediaSequence(manifest)

    if (manifest.includes('#EXT-X-ENDLIST')) {
      status = 'stale'
    } else if (seq !== undefined) {
      if (cached?.seq !== undefined && seq > cached.seq) {
        status = 'live'
      } else if (cached?.seq !== undefined && seq === cached.seq && now - (cached?.checkedAt ?? 0) > HLS_STALE_THRESHOLD_MS) {
        status = 'stale'
      } else {
        status = cached?.status ?? 'unknown'
      }
    }
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err)
    console.warn('[hls-probe] skipped for', stream.link, '-', message)
    if (cached?.status) {
      status = cached.status
      seq = cached.seq
    } else {
      status = 'live'
      seq = undefined
    }
  }

  hlsProbeCache.set(stream.link, {
    checkedAt: now,
    status,
    seq,
  })

  if (status !== 'unknown') {
    applyCachedStatus(status)
  }

  return changed
}

export async function probeIowaHlsStatus(streams: StreamList) {
  const now = Date.now()
  const candidates = streams.filter((s) => isIowaHls(s))
  let changed = false

  const queue = [...candidates]
  const worker = async () => {
    while (queue.length > 0) {
      const stream = queue.shift()
      if (!stream) break
      const didChange = await probeSingleIowa(stream, now)
      changed = changed || didChange
    }
  }

  const concurrency = Math.min(5, queue.length)
  await Promise.all(Array.from({ length: concurrency || 1 }, () => worker()))

  return changed
}

interface LocalStreamDataEvents {
  update: [StreamDataContent[]]
}

export class LocalStreamData extends EventEmitter<LocalStreamDataEvents> {
  dataByURL: Map<string, StreamDataContent>

  constructor(entries: StreamDataContent[] = []) {
    super()
    this.dataByURL = new Map()
    for (const entry of entries) {
      if (!entry.link) {
        continue
      }
      this.dataByURL.set(entry.link, entry)
    }
  }

  update(url: string, data: Partial<StreamDataContent>) {
    const existing = this.dataByURL.get(url)
    const kind = data.kind ?? existing?.kind ?? 'video'
    const updated: StreamDataContent = { ...existing, ...data, kind, link: url }
    this.dataByURL.set(data.link ?? url, updated)
    if (data.link != null && url !== data.link) {
      this.dataByURL.delete(url)
    }
    this._emitUpdate()
  }

  delete(url: string) {
    this.dataByURL.delete(url)
    this._emitUpdate()
  }

  _emitUpdate() {
    this.emit('update', [...this.dataByURL.values()])
  }

  gen(): AsyncGenerator<StreamDataContent[]> {
    return new Repeater(async (push, stop) => {
      await push([...this.dataByURL.values()])
      this.on('update', push)
      await stop
      this.off('update', push)
    })
  }
}

export class StreamIDGenerator {
  idMap: Map<string, string>
  idSet: Set<string>

  constructor() {
    this.idMap = new Map()
    this.idSet = new Set()
  }

  process(streams: StreamDataContent[]) {
    const { idMap, idSet } = this

    for (const stream of streams) {
      const { link, source, label, kind, crop, notes } = stream
      const signature = JSON.stringify({ link, source, label, kind, crop, notes })

      let streamId = idMap.get(signature)
      if (streamId == null) {
        let counter = 0
        let newId
        const idBase = source || label || link
        if (!idBase) {
          console.warn('skipping empty stream', stream)
          continue
        }
        const normalizedText = idBase
          .toLowerCase()
          .replace(/[^\w]/g, '')
          .replace(/^the|^https?(www)?/, '')
        do {
          const textPart = normalizedText.substr(0, 3).toLowerCase()
          const counterPart = counter === 0 && textPart ? '' : counter
          newId = `${textPart}${counterPart}`
          counter++
        } while (idSet.has(newId))

        streamId = newId
        idMap.set(signature, streamId)
        idSet.add(streamId)
      }

      stream._id = streamId
    }
    return streams
  }
}
