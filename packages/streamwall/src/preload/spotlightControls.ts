import { ipcRenderer, webFrame } from 'electron'
import { normalizeOverlayUrl, normalizeOverlayScale } from 'streamwall-shared'

// Spotlight lives above the grid's native label surface, so its own title and
// close target belong inside the enlarged media surface.
let spotlightControls: HTMLDivElement | undefined
let spotlightOverlay: HTMLIFrameElement | undefined
type SpotlightControls = {
  label?: string; labelPosition?: string; fontSize?: number
  overlayUrl?: string; overlayScale?: number; overlayVisible?: boolean
}
let latestControls: SpotlightControls | undefined
function updateSpotlightControls(payload: SpotlightControls) {
  if (!document.documentElement) return
  if (!spotlightControls) {
    try {
      webFrame.insertCSS(`
        #streamwall-tile-overlay {
          display: block !important; position: fixed !important;
          inset: 0 !important; width: 100vw !important; height: 100vh !important;
          border: none !important; background: transparent !important;
          z-index: 2147483646 !important; pointer-events: none !important;
          transform: scale(var(--sw-overlay-scale, 1)) !important;
          transform-origin: center center !important;
          visibility: var(--sw-overlay-visibility, visible) !important;
        }
        #streamwall-spotlight-controls {
          display: block !important; position: fixed !important;
          inset: 0 !important; z-index: 2147483647 !important;
          pointer-events: auto !important; cursor: pointer !important;
          background: transparent !important; box-sizing: border-box !important;
          border: 2px solid rgba(255,255,255,.6) !important;
        }
        #streamwall-spotlight-controls > span {
          display: block !important; position: absolute !important;
          top: var(--sw-title-top) !important; bottom: var(--sw-title-bottom) !important;
          left: var(--sw-title-left) !important; right: var(--sw-title-right) !important;
          z-index: 2147483647 !important; pointer-events: none !important;
          max-width: calc(100% - 30px) !important; box-sizing: border-box !important;
          font: 600 var(--sw-title-size)/1.2 sans-serif !important;
          padding: 4px 10px !important; border-radius: 4px !important;
          color: white !important; background: rgba(0,0,0,.5) !important;
          white-space: nowrap !important; overflow: hidden !important;
          text-overflow: ellipsis !important;
        }
        #streamwall-spotlight-controls > span:empty { display: none !important; }
      `, { cssOrigin: 'user' })
    } catch (error) {
      console.warn('[spotlight] controls style failed', error)
    }
    spotlightControls = document.createElement('div')
    spotlightControls.id = 'streamwall-spotlight-controls'
    spotlightControls.setAttribute('role', 'button')
    spotlightControls.setAttribute('aria-label', 'Close enlarged view')
    spotlightControls.appendChild(document.createElement('span'))
    spotlightControls.addEventListener('click', (event) => {
      event.preventDefault()
      event.stopImmediatePropagation()
      ipcRenderer.send('spotlight-close')
    }, true)
    document.documentElement.appendChild(spotlightControls)
  }
  spotlightControls.firstElementChild!.textContent = payload.label ?? ''
  const position = payload.labelPosition ?? 'top-left'
  spotlightControls.style.setProperty('--sw-title-top', position.startsWith('bottom') ? 'auto' : '10px')
  spotlightControls.style.setProperty('--sw-title-bottom', position.startsWith('bottom') ? '10px' : 'auto')
  spotlightControls.style.setProperty('--sw-title-left', position.endsWith('right') ? 'auto' : '10px')
  spotlightControls.style.setProperty('--sw-title-right', position.endsWith('right') ? '10px' : 'auto')
  const fontSize = typeof payload.fontSize === 'number' && Number.isFinite(payload.fontSize)
    ? Math.min(96, Math.max(8, payload.fontSize)) : 16
  spotlightControls.style.setProperty('--sw-title-size', `${fontSize}px`)
  const overlayUrl = normalizeOverlayUrl(payload.overlayUrl)
  if (!overlayUrl) {
    spotlightOverlay?.remove()
    spotlightOverlay = undefined
  } else {
    if (!spotlightOverlay) {
      spotlightOverlay = document.createElement('iframe')
      spotlightOverlay.id = 'streamwall-tile-overlay'
      spotlightOverlay.title = 'Tile HTML overlay'
      spotlightOverlay.sandbox.add('allow-scripts', 'allow-same-origin')
      spotlightOverlay.allow = 'autoplay'
      spotlightOverlay.referrerPolicy = 'no-referrer'
      spotlightOverlay.tabIndex = -1
      document.documentElement.appendChild(spotlightOverlay)
    }
    // State broadcasts must not reload graphics or restart their animations.
    if (spotlightOverlay.src !== overlayUrl) spotlightOverlay.src = overlayUrl
    spotlightOverlay.style.setProperty('--sw-overlay-scale', String(normalizeOverlayScale(payload.overlayScale) / 100))
    spotlightOverlay.style.setProperty('--sw-overlay-visibility', payload.overlayVisible === false ? 'hidden' : 'visible')
  }
}
ipcRenderer.on('spotlight-controls', (_event, payload: SpotlightControls) => {
  latestControls = payload
  updateSpotlightControls(payload)
})
document.addEventListener('DOMContentLoaded', () => {
  if (latestControls) updateSpotlightControls(latestControls)
}, { once: true })
