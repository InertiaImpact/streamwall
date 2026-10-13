import { spawn, type ChildProcessWithoutNullStreams } from 'child_process'
import { randomUUID } from 'crypto'
import { createReadStream } from 'fs'
import { mkdir, mkdtemp, rm, stat } from 'fs/promises'
import { createServer, type Server } from 'http'
import { tmpdir } from 'os'
import path from 'path'

const START_TIMEOUT_MS = 20_000
const RETRY_DELAY_MS = 5_000

export function isHdHomeRunURL(value: string) {
  try {
    const url = new URL(value)
    return (
      (url.protocol === 'http:' || url.protocol === 'https:') &&
      url.port === '5004' &&
      /^\/auto\/(?:v\d+(?:\.\d+)?|ch\d+)\/?$/i.test(url.pathname)
    )
  } catch {
    return false
  }
}

export function canCopyHdHomeRunVideo(probeOutput: string) {
  return /Stream #\d+:\d+(?:\[[^\]]+\])?(?:\([^)]*\))?: Video: h264\b/i.test(
    probeOutput,
  )
}

type Entry = {
  id: string
  source: string
  directory: string
  viewers: Set<number>
  process?: ChildProcessWithoutNullStreams
  retry?: NodeJS.Timeout
  ready: Promise<string>
  stopped: boolean
}

/** One local HLS conversion per channel, shared by all views displaying it. */
export class HdHomeRunBridge {
  private server?: Server
  private baseURL?: string
  private root?: string
  private entries = new Map<string, Entry>()
  private viewSources = new Map<number, string>()
  private opening?: Promise<void>

  constructor(private readonly ffmpegPath: string) {}

  async acquire(source: string, viewId: number) {
    if (this.viewSources.get(viewId) !== source) this.release(viewId)
    await this.open()
    const root = this.root
    if (!root) throw new Error('HDHomeRun server did not start')
    let entry = this.entries.get(source)
    if (!entry) {
      const id = randomUUID()
      entry = {
        id,
        source,
        directory: path.join(root, id),
        viewers: new Set(),
        ready: Promise.resolve(''),
        stopped: false,
      }
      this.entries.set(source, entry)
      entry.ready = this.start(entry)
    }
    entry.viewers.add(viewId)
    this.viewSources.set(viewId, source)
    try {
      return await entry.ready
    } catch (error) {
      this.release(viewId)
      throw error
    }
  }

  release(viewId: number) {
    const source = this.viewSources.get(viewId)
    if (!source) return
    this.viewSources.delete(viewId)
    const entry = this.entries.get(source)
    if (!entry) return
    entry.viewers.delete(viewId)
    if (entry.viewers.size === 0) this.stop(entry)
  }

  close() {
    this.entries.forEach((entry) => this.stop(entry))
    this.server?.close()
    this.server = undefined
    this.baseURL = undefined
    this.opening = undefined
    this.viewSources.clear()
    const root = this.root
    this.root = undefined
    if (root)
      void rm(root, {
        recursive: true,
        force: true,
        maxRetries: 5,
        retryDelay: 300,
      })
  }

  private async open() {
    if (this.baseURL) return
    if (!this.opening) {
      this.opening = (async () => {
        this.root = await mkdtemp(path.join(tmpdir(), 'streamwall-hdhomerun-'))
        const server = createServer((request, response) => {
          response.setHeader('Access-Control-Allow-Origin', '*')
          response.setHeader('Cache-Control', 'no-store')
          if (request.method !== 'GET' && request.method !== 'HEAD') {
            response.writeHead(405).end()
            return
          }
          const match = /^\/([a-f\d-]+)\/(index\.m3u8|seg\d+\.ts)$/.exec(
            request.url?.split('?')[0] ?? '',
          )
          const entry =
            match &&
            Array.from(this.entries.values()).find(
              (item) => item.id === match[1],
            )
          if (!entry || !match) {
            response.writeHead(404).end()
            return
          }
          const file = path.join(entry.directory, match[2])
          void stat(file)
            .then((fileInfo) => {
              response.setHeader(
                'Content-Type',
                match[2].endsWith('.m3u8')
                  ? 'application/vnd.apple.mpegurl'
                  : 'video/mp2t',
              )
              response.setHeader('Content-Length', fileInfo.size)
              response.writeHead(200)
              if (request.method === 'HEAD') response.end()
              else
                createReadStream(file)
                  .on('error', () => response.destroy())
                  .pipe(response)
            })
            .catch(() => response.writeHead(404).end())
        })
        await new Promise<void>((resolve, reject) => {
          server.once('error', reject)
          server.listen(0, '127.0.0.1', () => {
            server.off('error', reject)
            resolve()
          })
        })
        this.server = server
        const address = server.address()
        if (!address || typeof address === 'string')
          throw new Error('Unable to start HDHomeRun server')
        this.baseURL = `http://127.0.0.1:${address.port}`
      })()
    }
    return this.opening
  }

  private async start(entry: Entry): Promise<string> {
    await rm(entry.directory, {
      recursive: true,
      force: true,
      maxRetries: 5,
      retryDelay: 300,
    })
    await mkdir(entry.directory, { recursive: true })
    const codec = await this.probe(entry.source)
    if (entry.stopped) throw new Error('HDHomeRun view closed')
    const playlist = path.join(entry.directory, 'index.m3u8')
    const args = [
      '-hide_banner',
      '-nostdin',
      '-loglevel',
      'warning',
      '-i',
      entry.source,
      '-map',
      '0:v:0',
      '-map',
      '0:a:0?',
      ...(codec === 'h264'
        ? ['-c:v', 'copy']
        : [
            '-c:v',
            'libx264',
            '-preset',
            'veryfast',
            '-tune',
            'zerolatency',
            '-pix_fmt',
            'yuv420p',
            '-force_key_frames',
            'expr:gte(t,n_forced*2)',
          ]),
      '-c:a',
      'aac',
      // AAC 5.1(side) uses a PCE that hls.js cannot infer from the ADTS header.
      '-ac',
      '2',
      '-f',
      'hls',
      '-hls_time',
      '2',
      '-hls_list_size',
      '5',
      '-hls_start_number_source',
      'epoch',
      '-hls_flags',
      'delete_segments+temp_file',
      '-hls_segment_filename',
      path.join(entry.directory, 'seg%06d.ts'),
      playlist,
    ]
    const child = spawn(this.ffmpegPath, args, { windowsHide: true })
    entry.process = child
    let failure = ''
    child.stderr.on('data', (chunk: Buffer) => {
      failure = (failure + chunk.toString()).slice(-2000)
    })
    child.once('error', (error) => {
      failure = error.message
    })
    child.once('exit', () => {
      entry.process = undefined
      if (!entry.stopped && entry.viewers.size > 0) {
        console.warn('[HDHomeRun] converter exited:', failure)
        entry.retry = setTimeout(() => {
          entry.retry = undefined
          if (entry.stopped) return
          entry.ready = this.start(entry)
          void entry.ready.catch((error) =>
            console.warn('[HDHomeRun] restart failed:', error),
          )
        }, RETRY_DELAY_MS)
      }
    })

    const deadline = Date.now() + START_TIMEOUT_MS
    while (!entry.stopped && Date.now() < deadline) {
      try {
        if ((await stat(playlist)).size > 0) {
          return `${this.baseURL}/${entry.id}/index.m3u8`
        }
      } catch {
        /* Waiting for the first segment. */
      }
      if (!entry.process)
        throw new Error(`HDHomeRun converter stopped: ${failure}`)
      await new Promise((resolve) => setTimeout(resolve, 200))
    }
    child.kill()
    throw new Error(
      `HDHomeRun playback did not start: ${failure || 'timed out'}`,
    )
  }

  private async probe(source: string): Promise<'h264' | 'transcode'> {
    return new Promise((resolve) => {
      const child = spawn(
        this.ffmpegPath,
        [
          '-hide_banner',
          '-nostdin',
          '-i',
          source,
          '-map',
          '0:v:0',
          '-frames:v',
          '0',
          '-f',
          'null',
          '-',
        ],
        { windowsHide: true },
      )
      let output = ''
      const timer = setTimeout(() => child.kill(), 10_000)
      child.stderr.on('data', (chunk: Buffer) => {
        output = (output + chunk.toString()).slice(-10_000)
      })
      child.once('error', () => resolve('transcode'))
      child.once('close', () => {
        clearTimeout(timer)
        resolve(canCopyHdHomeRunVideo(output) ? 'h264' : 'transcode')
      })
    })
  }

  private stop(entry: Entry) {
    entry.stopped = true
    if (entry.retry) clearTimeout(entry.retry)
    entry.process?.kill()
    this.entries.delete(entry.source)
    void rm(entry.directory, {
      recursive: true,
      force: true,
      maxRetries: 5,
      retryDelay: 300,
    })
  }
}
