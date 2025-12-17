import { contextBridge, ipcRenderer, IpcRendererEvent } from 'electron'
import { StreamwallState } from 'streamwall-shared'

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
  onSpotlight: (handleSpotlight: (url: string, streamId?: string) => void) => {
    const internalHandler = (
      _ev: IpcRendererEvent,
      payload: { url?: string; streamId?: string },
    ) => handleSpotlight(payload?.url, payload?.streamId)
    ipcRenderer.on('spotlight', internalHandler)
    return () => {
      ipcRenderer.off('spotlight', internalHandler)
    }
  },
  spotlight: (url?: string, streamId?: string) =>
    ipcRenderer.send('spotlight-local', { url, streamId }),
}

export type StreamwallLayerGlobal = typeof api

contextBridge.exposeInMainWorld('streamwallLayer', api)
