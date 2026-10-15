// Exercise the built preload in Electron, including APIs that a Vite build
// cannot type-check. Uses generated media and an isolated temporary profile.
const assert = require('node:assert/strict')
const fs = require('node:fs')
const os = require('node:os')
const path = require('node:path')
if (!process.versions.electron || process.env.ELECTRON_RUN_AS_NODE) {
  const env = { ...process.env }
  delete env.ELECTRON_RUN_AS_NODE
  const result = require('node:child_process').spawnSync(
    require('electron'), [__filename, ...process.argv.slice(2)],
    { env, stdio: 'inherit', windowsHide: true },
  )
  if (result.error) console.error(result.error)
  process.exit(result.status ?? 1)
}
const { app, BrowserWindow, ipcMain } = require('electron')

const preload = path.resolve(process.argv[2] ||
  path.join(__dirname, '../.vite/build/mediaPreload.js'))
const scratch = fs.mkdtempSync(path.join(os.tmpdir(), 'streamwall-preload-'))
app.setPath('userData', path.join(scratch, 'profile'))
app.commandLine.appendSwitch('autoplay-policy', 'no-user-gesture-required')
app.on('window-all-closed', () => {})
app.on('quit', () => {
  // Chromium may briefly retain a profile lock on Windows.
  try { fs.rmSync(scratch, { recursive: true, force: true }) } catch {}
})

const fixture = `<!doctype html><title>Preload check</title>
<video autoplay muted></video><audio autoplay muted></audio>
<script>
const canvas = document.createElement('canvas');
canvas.width = 320; canvas.height = 180;
const ctx = canvas.getContext('2d');
let frame = 0;
function draw() {
  ctx.fillStyle = frame++ % 2 ? 'blue' : 'green';
  ctx.fillRect(0, 0, canvas.width, canvas.height);
}
draw(); setInterval(draw, 100);
document.querySelector('video').srcObject = canvas.captureStream(10);
const audioContext = new AudioContext();
const oscillator = audioContext.createOscillator();
const destination = audioContext.createMediaStreamDestination();
oscillator.connect(destination); oscillator.start();
document.querySelector('audio').srcObject = destination.stream;
audioContext.resume();
</script>`
fs.writeFileSync(path.join(scratch, 'media.html'), fixture)
fs.writeFileSync(path.join(scratch, 'playHLS.html'), fixture)
fs.writeFileSync(path.join(scratch, 'web.html'), '<!doctype html><title>Web check</title><body>Web source</body>')

const pending = new Map()
const closed = new Set()
ipcMain.on('spotlight-close', (event) => closed.add(event.sender.id))
ipcMain.handle('view-init', (event) => ({
  content: { kind: pending.get(event.sender.id).kind },
  options: {}, mediaHealthEnabled: false, nearbyFallbackEnabled: false,
}))
ipcMain.on('view-loaded', (event) => pending.get(event.sender.id)?.resolve())
ipcMain.on('view-error', (event, detail) =>
  pending.get(event.sender.id)?.reject(new Error(JSON.stringify(detail))))

function waitForRenderer(contents, condition) {
  return contents.executeJavaScript(`new Promise((resolve, reject) => {
    const started = Date.now();
    const timer = setInterval(() => {
      if (${condition}) { clearInterval(timer); resolve(true); }
      else if (Date.now() - started > 3000) {
        clearInterval(timer); reject(new Error('Renderer condition timed out'));
      }
    }, 25);
  })`)
}

async function check({ name, kind, file, bridge = false }) {
  const win = new BrowserWindow({
    show: false,
    webPreferences: {
      preload, nodeIntegration: false, contextIsolation: true,
      backgroundThrottling: false,
    },
  })
  const contents = win.webContents
  contents.audioMuted = true
  let timeout
  const loaded = new Promise((resolve, reject) => {
    pending.set(contents.id, { kind, resolve, reject })
    timeout = setTimeout(() => reject(new Error(`${name}: preload did not report ready`)), 15000)
    contents.on('preload-error', (_event, _path, error) => reject(error))
    contents.on('render-process-gone', (_event, details) => reject(new Error(JSON.stringify(details))))
  })
  try {
    await Promise.all([contents.loadFile(path.join(scratch, file)), loaded])
    if (bridge) {
      assert.equal(await contents.executeJavaScript(
        "typeof window.streamwallHlsRequests?.acquire"), 'function')
    }
    if (kind !== 'web') {
      const tag = kind === 'audio' ? 'audio' : 'video'
      await waitForRenderer(contents, `!document.querySelector('${tag}').paused &&
        document.querySelector('${tag}').currentTime > 0.1`)
    }
    contents.send('spotlight-blur', true)
    await waitForRenderer(contents, "getComputedStyle(document.documentElement).filter === 'blur(8px)'")
    contents.send('spotlight-blur', false)
    await waitForRenderer(contents, "getComputedStyle(document.documentElement).filter === 'none'")
    contents.send('spotlight-controls', { label: 'Enlarged test feed', labelPosition: 'bottom-right', fontSize: 20 })
    await waitForRenderer(contents, "document.querySelector('#streamwall-spotlight-controls > span')?.textContent === 'Enlarged test feed'")
    const controls = await contents.executeJavaScript(`(() => {
      const root=document.getElementById('streamwall-spotlight-controls'), title=root.firstElementChild;
      return { display:getComputedStyle(root).display, position:getComputedStyle(root).position,
        titleDisplay:getComputedStyle(title).display, bottom:getComputedStyle(title).bottom,
        right:getComputedStyle(title).right, font:getComputedStyle(title).fontSize };
    })()`)
    assert.deepEqual(controls, { display: 'block', position: 'fixed', titleDisplay: 'block', bottom: '10px', right: '10px', font: '20px' })
    await contents.executeJavaScript("document.getElementById('streamwall-spotlight-controls').click()")
    await new Promise((resolve) => setTimeout(resolve, 50))
    assert.ok(closed.has(contents.id), 'Enlarged player click must request close')
    console.log(`PASS ${name}: ready, playback/bridge, blur/restore, enlarged title and close`)
  } finally {
    clearTimeout(timeout)
    pending.delete(contents.id)
    closed.delete(contents.id)
    win.destroy()
  }
}

app.whenReady().then(async () => {
  assert.ok(fs.existsSync(preload), `Build the media preload first: ${preload}`)
  for (const scenario of [
    { name: 'Web', kind: 'web', file: 'web.html' },
    { name: 'Direct video', kind: 'video', file: 'media.html' },
    { name: 'HLS player', kind: 'video', file: 'playHLS.html', bridge: true },
    { name: 'HDHomeRun player', kind: 'hdhomerun', file: 'playHLS.html', bridge: true },
    { name: 'Audio', kind: 'audio', file: 'media.html' },
  ]) await check(scenario)
  app.exit(0)
}).catch((error) => {
  console.error('FAIL media preload:', error)
  app.exit(1)
})
