// Integration check for spotlight using the real window manager, preloads and
// built overlay. Generated local video keeps the check independent of cameras.
const assert = require('node:assert/strict')
const fs = require('node:fs')
const path = require('node:path')
const { pathToFileURL } = require('node:url')
const root = path.resolve(__dirname, '..')
const scratch = path.join(root, 'out/spotlight-check')
fs.mkdirSync(scratch, { recursive: true })
if (!process.versions.electron || process.env.ELECTRON_RUN_AS_NODE) {
  require('esbuild').buildSync({
    entryPoints: [path.join(root, 'src/main/StreamWindow.ts')],
    outfile: path.join(scratch, 'runtime.cjs'), bundle: true,
    platform: 'node', format: 'cjs', external: ['electron'],
    define: {
      MAIN_WINDOW_VITE_DEV_SERVER_URL: JSON.stringify(pathToFileURL(path.join(root, '.vite/renderer/main_window')).href),
      MAIN_WINDOW_VITE_NAME: JSON.stringify('main_window'),
    },
  })
  for (const preload of ['layerPreload.js', 'mediaPreload.js']) {
    fs.copyFileSync(path.join(root, '.vite/build', preload), path.join(scratch, preload))
  }
  const env = { ...process.env }
  delete env.ELECTRON_RUN_AS_NODE
  const result = require('node:child_process').spawnSync(require('electron'), [__filename],
    { env, stdio: 'inherit', windowsHide: true })
  if (result.error) console.error(result.error)
  process.exit(result.status ?? 1)
}
const { app, BrowserWindow, nativeImage } = require('electron')
app.setPath('userData', path.join(scratch, 'profile'))
app.commandLine.appendSwitch('autoplay-policy', 'no-user-gesture-required')
app.on('window-all-closed', () => {})
BrowserWindow.prototype.show = function () {} // Keep all check windows hidden.
const pause = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
async function until(condition, label) {
  const started = Date.now()
  while (!await condition()) {
    if (Date.now() - started > 10000) throw new Error(`Timed out: ${label}`)
    await pause(25)
  }
}
const fixture = path.join(scratch, 'media.html')
fs.writeFileSync(fixture, `<!doctype html><video autoplay muted></video><script>
const canvas=document.createElement('canvas'); canvas.width=320; canvas.height=180;
const ctx=canvas.getContext('2d'); let frame=0;
function draw(){ctx.fillStyle=frame++%2?'blue':'green';ctx.fillRect(0,0,320,180)}
draw();setInterval(draw,100);document.querySelector('video').srcObject=canvas.captureStream(10);
</script>`)

app.whenReady().then(async () => {
  const StreamWindow = require(path.join(scratch, 'runtime.cjs')).default
  const fixtureServer = require('node:http').createServer((_request, response) => {
    response.writeHead(200, { 'Content-Type': 'text/html' })
    response.end(fs.readFileSync(fixture))
  })
  await new Promise((resolve) => fixtureServer.listen(0, '127.0.0.1', resolve))
  const config = { width: 1120, height: 630, cols: 7, rows: 7,
    backgroundColor: '#ffffff', activeColor: '#ff0000' }
  const grid = new StreamWindow(config)
  const content = { url: `http://127.0.0.1:${fixtureServer.address().port}/media`, kind: 'video', streamId: 'cam24' }
  const pos = { x: 480, y: 270, width: 160, height: 90, spaces: [24] }
  const streams = Array.from({ length: 49 }, (_, i) => ({
    _id: `cam${i}`, link: i === 24 ? content.url : `https://example.com/${i}`, label: `GRID LABEL ${i}`,
  }))
  const otherViews = streams.flatMap((stream, i) => i === 24 ? [] : [{
    state: { displaying: { running: { video: 'normal', audio: 'muted' } } },
    context: { id: i, content: { streamId: stream._id, url: stream.link, kind: 'video' },
      pos: { x: (i % 7) * 160, y: Math.floor(i / 7) * 90, width: 160, height: 90, spaces: [i] } },
  }])
  const state = { config, streams, overlayLabelFontSize: 16, views: otherViews }
  grid.on('load', () => grid.onState(state))
  grid.on('state', (views) => {
    state.views = [...otherViews, ...views]
    grid.onState(state)
  })
  const source = grid.createView(content)
  grid.views.set(source.getSnapshot().context.id, source)
  const sourceContents = source.getSnapshot().context.view.webContents
  sourceContents.audioMuted = true
  let navigations = 0
  sourceContents.on('did-start-navigation', () => navigations++)
  source.send({ type: 'OPTIONS', options: {} })
  source.send({ type: 'DISPLAY', content, pos })
  await until(() => source.getSnapshot().matches({ displaying: 'running' }), 'source playback')
  await until(() => grid.overlayView.webContents.executeJavaScript(
    "document.body.textContent.includes('GRID LABEL 29')"), 'overlay startup')
  await pause(250)

  // Hidden WebContentsViews may not submit native capture surfaces. Supply an
  // actual frame from the generated video to exercise the preview branch.
  sourceContents.capturePage = async () => nativeImage.createFromDataURL(
    await sourceContents.executeJavaScript(`(() => {
      const video=document.querySelector('video'), canvas=document.createElement('canvas');
      canvas.width=160; canvas.height=90;
      canvas.getContext('2d').drawImage(video,0,0,160,90);
      return canvas.toDataURL();
    })()`),
  )

  const started = Date.now()
  grid.toggleSpotlight(content.url, content.streamId)
  const duplicate = grid.spotlightActor.getSnapshot().context.view
  const boundsChanges = []
  const setBounds = duplicate.setBounds.bind(duplicate)
  duplicate.setBounds = (bounds) => { boundsChanges.push(bounds); return setBounds(bounds) }
  await until(() => !!grid.spotlightSnapshot, 'instant preview capture')
  const previewMs = Date.now() - started
  await until(() => grid.spotlightPhase === 'open', 'live spotlight')
  await pause(400)
  assert.deepEqual(source.getSnapshot().context.view.getBounds(), { x: 480, y: 270, width: 160, height: 90 })
  assert.deepEqual(duplicate.getBounds(), { x: 112, y: 63, width: 896, height: 504 })
  assert.ok(boundsChanges.length <= 3, `Repeated native resizing: ${boundsChanges.length} updates`)
  assert.equal(navigations, 1, 'Spotlight must not restart the original stream')

  // Pixel checks catch the reported bug even when DOM labels still exist.
  // A hidden standalone window submits a capture surface even when native
  // child views do not. Render the same built overlay and IPC state there.
  const overlayWindow = new BrowserWindow({
    width: 1120, height: 630, useContentSize: true, show: false,
    backgroundColor: '#ffffff', webPreferences: {
      preload: path.join(scratch, 'layerPreload.js'), contextIsolation: true,
      backgroundThrottling: false,
    },
  })
  const overlay = overlayWindow.webContents
  const nativeOverlay = grid.overlayView
  grid.overlayView = { webContents: overlay }
  StreamWindow.registerSender(overlay.id, grid)
  await overlay.loadFile(path.join(root, '.vite/renderer/main_window/src/renderer/overlay.html'))
  await until(() => overlay.executeJavaScript("document.body.textContent.includes('GRID LABEL 29')"), 'visual overlay')
  await pause(400)
  grid.overlayView = nativeOverlay
  const capture = await overlay.capturePage()
  fs.writeFileSync(path.join(scratch, 'overlay.png'), capture.toPNG())
  const pixels = capture.toBitmap()
  const imageWidth = capture.getSize().width
  const imageHeight = capture.getSize().height
  const regions = await overlay.executeJavaScript(`['GRID LABEL 0','GRID LABEL 29'].map(text => {
    const el=[...document.querySelectorAll('span')].find(e=>e.textContent===text);
    return el.getBoundingClientRect().toJSON();
  })`)
  function hasDarkPixels(rect) {
    const sx = imageWidth / 1120, sy = imageHeight / 630
    for (let y = Math.ceil((rect.top + 2) * sy); y < (rect.bottom - 2) * sy; y++) {
      for (let x = Math.ceil((rect.left + 2) * sx); x < (rect.right - 2) * sx; x++) {
        const i = (y * imageWidth + x) * 4
        if (pixels[i + 3] && pixels[i] < 220 && pixels[i + 1] < 220 && pixels[i + 2] < 220) return true
      }
    }
    return false
  }
  assert.ok(hasDarkPixels(regions[0]), 'Labels outside spotlight must remain visible')
  assert.ok(!hasDarkPixels(regions[1]), 'Grid labels must not paint across spotlight')
  const frameBounds = await overlay.executeJavaScript(`(() => {
    const title=[...document.querySelectorAll('span')].find(e=>e.textContent==='GRID LABEL 24');
    return title.parentElement.parentElement.getBoundingClientRect().toJSON();
  })()`)
  assert.ok(Math.abs(frameBounds.x - 112) < 1 && Math.abs(frameBounds.width - 896) < 1,
    'Live spotlight chrome must reach its final size even without renderer animation frames')
  overlay.send('state', { ...state, views: state.views.map((view) =>
    view.context.content?.streamId === content.streamId
      ? { ...view, state: { displaying: 'error' } } : view) })
  await pause(100)
  assert.equal(await overlay.executeJavaScript(
    "[...document.querySelectorAll('span')].some(e=>e.textContent==='GRID LABEL 24')"), true,
  'A source-tile error must not remove the healthy spotlight frame and label mask')

  grid.toggleSpotlight(content.url, content.streamId)
  await until(() => grid.spotlightPhase === null, 'close animation cleanup')
  assert.equal(grid.spotlightActor, null)
  assert.equal(source.getSnapshot().matches('displaying.running.video.normal'), true)
  assert.equal(navigations, 1)
  grid.toggleSpotlight(content.url, content.streamId)
  grid.toggleSpotlight(content.url, content.streamId)
  await until(() => grid.spotlightPhase === null, 'rapid cancel cleanup')
  assert.equal(grid.spotlightActor, null)
  assert.equal(grid.views.size, 1)
  console.log(`PASS spotlight: preview ${previewMs}ms, ${boundsChanges.length} native bounds updates; outside labels visible, inside labels clipped, source preserved, close/cancel cleaned up`)
  app.exit(0)
}).catch((error) => { console.error('FAIL spotlight:', error); app.exit(1) })
