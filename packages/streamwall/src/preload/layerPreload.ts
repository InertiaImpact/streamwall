import { contextBridge, ipcRenderer, IpcRendererEvent } from 'electron'
import { StreamwallState } from 'streamwall-shared'

type SpotlightPayload = {
  url?: string
  streamId?: string
  phase?: 'preparing' | 'open' | 'closing' | null
  snapshot?: string
}
let latestSpotlight: SpotlightPayload | undefined
// State can arrive before the overlay component mounts its listener.
ipcRenderer.on('spotlight', (_event, payload: SpotlightPayload) => {
  latestSpotlight = payload
})

const api = {
  openDevTools: () => ipcRenderer.send('devtools-overlay'),
  load: () => ipcRenderer.invoke('layer:load'),
  expandView: (url?: string) => ipcRenderer.send('view-expand', { url }),
  onState: (handleState: (state: StreamwallState) => void) => {
    const internalHandler = (_ev: IpcRendererEvent, state: StreamwallState) =>
      handleState(state)
    ipcRenderer.on('state', internalHandler)
    return () => {
      ipcRenderer.off('state', internalHandler)
    }
  },
  onSpotlight: (handleSpotlight: (
    url?: string,
    streamId?: string,
    phase?: 'preparing' | 'open' | 'closing' | null,
    snapshot?: string,
  ) => void) => {
    const internalHandler = (
      _ev: IpcRendererEvent,
      payload: SpotlightPayload,
    ) => handleSpotlight(payload?.url, payload?.streamId, payload?.phase, payload?.snapshot)
    ipcRenderer.on('spotlight', internalHandler)
    if (latestSpotlight) {
      handleSpotlight(latestSpotlight.url, latestSpotlight.streamId,
        latestSpotlight.phase, latestSpotlight.snapshot)
    }
    return () => {
      ipcRenderer.off('spotlight', internalHandler)
    }
  },
  spotlight: (url?: string, streamId?: string) =>
    ipcRenderer.send('spotlight-local', { url, streamId }),
  spotlightFrameReady: () => ipcRenderer.send('spotlight-frame-ready'),
}

export type StreamwallLayerGlobal = typeof api

contextBridge.exposeInMainWorld('streamwallLayer', api)
