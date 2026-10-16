import Color from 'color'
import { render } from 'preact'
import { useCallback, useEffect, useMemo, useState } from 'preact/hooks'
import { useHotkeys } from 'react-hotkeys-hook'
import {
  FaFacebook,
  FaInstagram,
  FaTiktok,
  FaTwitch,
  FaVolumeUp,
  FaYoutube,
} from 'react-icons/fa'
import { RiKickFill, RiTwitterXFill } from 'react-icons/ri'
import { StreamwallState, normalizeOverlayUrl, normalizeOverlayScale, isTileOverlayVisible } from 'streamwall-shared'
import { keyframes, styled } from 'styled-components'
import { TailSpin } from 'svg-loaders-react'
import { matchesState } from 'xstate'
import packageInfo from '../../package.json'
import { StreamwallLayerGlobal } from '../preload/layerPreload'

import '@fontsource/noto-sans'
import 'streamwall-control-ui/src/index.css'

declare global {
  interface Window {
    streamwallLayer: StreamwallLayerGlobal
  }
}

function Overlay({
  config,
  views,
  streams,
  overlayLabelFontSize,
  gridId,
  uiState,
}: Pick<StreamwallState, 'config' | 'views' | 'streams' | 'overlayLabelFontSize' | 'gridId' | 'uiState'>) {
  const { width, height, activeColor } = config
  const [expandedUrl, setExpandedUrl] = useState<string | undefined>()
  const [expandedStreamId, setExpandedStreamId] = useState<string | undefined>()
  const [spotlightPhase, setSpotlightPhase] = useState<
    'preparing' | 'open' | 'closing' | null
  >(null)
  const [spotlightSnapshot, setSpotlightSnapshot] = useState<string | undefined>()
  const [spotlightOverlaySnapshot, setSpotlightOverlaySnapshot] = useState<string | undefined>()
  const [loadedVideoSnapshot, setLoadedVideoSnapshot] = useState<string | undefined>()
  const [loadedOverlaySnapshot, setLoadedOverlaySnapshot] = useState<string | undefined>()
  useEffect(() => {
    if (spotlightPhase === 'closing' && spotlightSnapshot && loadedVideoSnapshot === spotlightSnapshot &&
        (!spotlightOverlaySnapshot || loadedOverlaySnapshot === spotlightOverlaySnapshot)) {
      window.streamwallLayer.spotlightFrameReady()
    }
  }, [spotlightPhase, spotlightSnapshot, spotlightOverlaySnapshot, loadedVideoSnapshot, loadedOverlaySnapshot])

  const toggleSpotlight = useCallback((streamId?: string, url?: string) => {
    window.streamwallLayer.spotlight(url, streamId)
  }, [])
  
  // Listen for spotlight messages from the main process
  useEffect(() => {
    console.debug('Setting up spotlight listener')
    const unsubscribe = window.streamwallLayer.onSpotlight(
      (
        url?: string,
        streamId?: string,
        phase?: 'preparing' | 'open' | 'closing' | null,
        snapshot?: string,
        overlaySnapshot?: string,
      ) => {
        console.debug('Received spotlight message for URL:', url, streamId)
        setExpandedUrl(url)
        setExpandedStreamId(streamId)
        setSpotlightPhase(phase ?? null)
        setSpotlightSnapshot(snapshot)
        setSpotlightOverlaySnapshot(overlaySnapshot)
      },
    )
    return unsubscribe
  }, [])
  
  // Memoize stream lookup map for faster access
  const streamsByUrl = useMemo(() => {
    const map = new Map<string, any[]>()
    streams.forEach((s) => {
      const arr = map.get(s.link) ?? []
      arr.push(s)
      map.set(s.link, arr)
    })
    return map
  }, [streams])

  const streamsById = useMemo(() => {
    const map = new Map<string, any>()
    streams.forEach((s) => s._id && map.set(s._id, s))
    return map
  }, [streams])
  
  const expandedKey = expandedStreamId ?? expandedUrl
  
  const activeViews = views.filter(
    ({ state }) =>
      matchesState('displaying', state) &&
      !matchesState('displaying.error', state),
  )
  // A healthy enlarged player can outlive a reload/error in its source tile.
  // Keep its transition state until the main process ends spotlight.
  const expandedView = views.find(({ context }) =>
    expandedKey === (context.content?.streamId ?? context.content?.url),
  )
  const expandedContent = expandedView?.context.content
  const expandedPos = expandedView?.context.pos
  const expandedData = expandedContent?.streamId
    ? streamsById.get(expandedContent.streamId)
    : expandedContent?.url ? streamsByUrl.get(expandedContent.url)?.[0] : undefined
  const spotlightVisible = !!expandedContent && !!expandedPos &&
    spotlightPhase != null && (spotlightPhase !== 'preparing' || !!spotlightSnapshot)

  const overlays = streams.filter((s) => s.kind === 'overlay')
  return (
    <OverlayContainer>
      <VersionFooter />
      <GridAnnotations>
      {activeViews.map(({ state, context }) => {
        const { content, pos } = context
        if (!content || !pos) {
          return
        }

        const data = content.streamId
          ? streamsById.get(content.streamId)
          : streamsByUrl.get(content.url)?.[0]
        const original = context.fallbackOriginalStreamId
          ? streamsById.get(context.fallbackOriginalStreamId)
          : undefined
        const isListening = matchesState(
          'displaying.running.audio.listening',
          state,
        )
        const isBackgroundListening = matchesState(
          'displaying.running.audio.background',
          state,
        )
        const isBlurred = matchesState(
          'displaying.running.video.blurred',
          state,
        )
        const isLoading = matchesState('displaying.loading', state)
        const hasTitle = data && (data.label || data.source)
        const position = data?.labelPosition ?? 'top-left'
        const isPreparing = expandedKey === (content.streamId ?? content.url) &&
          spotlightPhase === 'preparing' && !spotlightVisible
        // Its media view supplies the blur; the enlarged native surface sits
        // above this entire label layer.
        const isObscured = spotlightVisible &&
          expandedKey === (content.streamId ?? content.url)
        return (
          <div key={`${pos.spaces.join('-')}`}>
            <SpaceBorder
              pos={pos}
              windowWidth={width}
              windowHeight={height}
              activeColor={activeColor}
              isListening={isListening}
              isObscured={isObscured}
              onClick={() => toggleSpotlight(content.streamId, content.url)}
              style={{ cursor: 'pointer' }}
            >
              {normalizeOverlayUrl(data?.overlayUrl) && (
                <TileOverlayClip style={{ visibility: isTileOverlayVisible(
                  uiState?.overlayHiddenCells, gridId, pos.spaces,
                ) ? 'visible' : 'hidden' }}>
                <TileOverlay
                  src={normalizeOverlayUrl(data?.overlayUrl)}
                  title="Tile HTML overlay"
                  sandbox="allow-scripts allow-same-origin"
                  allow="autoplay"
                  referrerPolicy="no-referrer"
                  tabIndex={-1}
                  style={{
                    filter: isObscured || isBlurred ? 'blur(15px)' : undefined,
                    transform: `scale(${normalizeOverlayScale(data?.overlayScale) / 100})`,
                  }}
                />
                </TileOverlayClip>
              )}
              {!isObscured && isBlurred && <BlurCover />}
              {!isObscured && hasTitle && (
                <StreamTitle
                  position={position}
                  activeColor={activeColor}
                  fontSize={overlayLabelFontSize}
                  isListening={isListening}
                >
                  <StreamIcon url={content.url} />
                  <span>
                    {original && <span title={`Temporary replacement for ${original.label ?? original.source ?? 'camera'}`}>↪ </span>}
                    {data.label ? (
                      data.label
                    ) : (
                      <>
                        {data.source} &ndash; {data.city} {data.state}
                      </>
                    )}
                  </span>
                  {(isListening || isBackgroundListening) && <FaVolumeUp />}
                </StreamTitle>
              )}
              {!isObscured && isLoading && <LoadingSpinner />}
              {isPreparing && <PreparingLabel>Opening enlarged view…</PreparingLabel>}
            </SpaceBorder>
          </div>
        )
      })}
      </GridAnnotations>
      {expandedContent && expandedPos && spotlightVisible && spotlightPhase !== 'open' && (
        <SpotlightFrame
          key={expandedKey}
          $phase={spotlightPhase}
          style={{
            '--spotlight-x': `${expandedPos.x - width * 0.1}px`,
            '--spotlight-y': `${expandedPos.y - height * 0.1}px`,
            '--spotlight-scale-x': expandedPos.width / (width * 0.8),
            '--spotlight-scale-y': expandedPos.height / (height * 0.8),
          }}
          onClick={() => toggleSpotlight(
            expandedContent.streamId,
            expandedContent.url,
          )}
        >
          {spotlightSnapshot && spotlightPhase !== 'open' && (
            <SpotlightSnapshot
              src={spotlightSnapshot}
              alt=""
              onLoad={() => setLoadedVideoSnapshot(spotlightSnapshot)}
            />
          )}
          {spotlightOverlaySnapshot && (
            <SpotlightSnapshot src={spotlightOverlaySnapshot} alt=""
              onLoad={() => setLoadedOverlaySnapshot(spotlightOverlaySnapshot)} />
          )}
          {spotlightPhase === 'preparing' && <PreparingLabel>Starting live stream…</PreparingLabel>}
          {!spotlightOverlaySnapshot && expandedData && (expandedData.label || expandedData.source) && (
            <StreamTitle
              position={expandedData.labelPosition ?? 'top-left'}
              activeColor={activeColor}
              fontSize={overlayLabelFontSize}
              isListening={false}
            >
              <StreamIcon url={expandedContent.url} />
              <span>{expandedData.label ?? expandedData.source}</span>
            </StreamTitle>
          )}
        </SpotlightFrame>
      )}
      {overlays.map((s) => (
        <OverlayIFrame
          key={s._id}
          src={s.link}
          sandbox="allow-scripts allow-same-origin"
          allow="autoplay"
          scrolling="no"
        />
      ))}
    </OverlayContainer>
  )
}

function App() {
  const [state, setState] = useState<StreamwallState | undefined>()

  useEffect(() => {
    const unsubscribe = window.streamwallLayer.onState(setState)
    window.streamwallLayer.load()
    return unsubscribe
  }, [])

  useHotkeys('ctrl+shift+i', () => {
    window.streamwallLayer.openDevTools()
  })

  if (!state) {
    return
  }

  const { config, views, streams, overlayLabelFontSize, gridId, uiState } = state
  return (
    <Overlay
      config={config}
      views={views}
      streams={streams}
      overlayLabelFontSize={overlayLabelFontSize}
      gridId={gridId}
      uiState={uiState}
    />
  )
}

function VersionFooter() {
  const [isShowing, setShowing] = useState(false)
  useEffect(() => {
    let timeout: ReturnType<typeof setTimeout> | undefined = undefined
    const interval = setInterval(() => {
      setShowing(true)
      timeout = setTimeout(() => {
        setShowing(false)
      }, 5000)
    }, 30 * 1000)
    return () => {
      clearInterval(interval)
      clearTimeout(timeout)
    }
  }, [])
  return (
    <VersionText isShowing={isShowing}>
      <strong>streamwall.io</strong> {packageInfo.version}
    </VersionText>
  )
}

function StreamIcon({ url }: { url: string }) {
  let parsedURL
  try {
    parsedURL = new URL(url)
  } catch {
    return null
  }

  let { host } = parsedURL
  host = host.replace(/^www\./, '')
  if (host === 'youtube.com' || host === 'youtu.be') {
    return <FaYoutube />
  } else if (host === 'facebook.com' || host === 'm.facebook.com') {
    return <FaFacebook />
  } else if (host === 'twitch.tv') {
    return <FaTwitch />
  } else if (host === 'instagram.com') {
    return <FaInstagram />
  } else if (host === 'tiktok.com') {
    return <FaTiktok />
  } else if (host === 'kick.com') {
    return <RiKickFill />
  } else if (host === 'x.com') {
    return <RiTwitterXFill />
  }
  return null
}

const OverlayContainer = styled.div`
  overflow: hidden;
`

const GridAnnotations = styled.div`
  position: fixed;
  inset: 0;
  pointer-events: none;
`

const SpaceBorder = styled.div.attrs(() => ({
  borderWidth: 2,
}))`
  display: flex;
  align-items: flex-start;
  position: absolute;
  left: ${({ pos }) => `${pos.x}px`};
  top: ${({ pos }) => `${pos.y}px`};
  width: ${({ pos }) => `${pos.width}px`};
  height: ${({ pos }) => `${pos.height}px`};
  border: 0 solid transparent;
  border-left-width: ${({ pos, borderWidth, isObscured }) =>
    (isObscured || pos.x === 0) ? 0 : borderWidth}px;
  border-right-width: ${({ pos, borderWidth, windowWidth, isObscured }) =>
    (isObscured || pos.x + pos.width === windowWidth) ? 0 : borderWidth}px;
  border-top-width: ${({ pos, borderWidth, isObscured }) =>
    (isObscured || pos.y === 0) ? 0 : borderWidth}px;
  border-bottom-width: ${({ pos, borderWidth, windowHeight, isObscured }) =>
    (isObscured || pos.y + pos.height === windowHeight) ? 0 : borderWidth}px;
  box-shadow: ${({ isListening, activeColor, isObscured }) =>
    !isObscured && isListening ? `0 0 10px ${activeColor} inset` : 'none'};
  box-sizing: border-box;
  cursor: pointer;
  transition: none;
  z-index: 1;
  pointer-events: auto;
  user-select: none;
`

const spotlightGrow = keyframes`
  from { transform: translate(var(--spotlight-x), var(--spotlight-y))
    scale(var(--spotlight-scale-x), var(--spotlight-scale-y)); }
  to { transform: translate(0, 0) scale(1, 1); }
`

const spotlightShrink = keyframes`
  from { transform: translate(0, 0) scale(1, 1); }
  to { transform: translate(var(--spotlight-x), var(--spotlight-y))
    scale(var(--spotlight-scale-x), var(--spotlight-scale-y)); }
`

const SpotlightFrame = styled.div<{ $phase: 'preparing' | 'open' | 'closing' | null }>`
  position: fixed;
  left: 10vw;
  top: 10vh;
  width: 80vw;
  height: 80vh;
  z-index: 1000;
  box-sizing: border-box;
  border: 2px solid rgba(255, 255, 255, 0.6);
  box-shadow: 0 0 12px rgba(0, 0, 0, 0.6);
  transform-origin: top left;
  animation: ${({ $phase }) => $phase === 'open' ? 'none' :
    $phase === 'closing' ? spotlightShrink : spotlightGrow}
    300ms cubic-bezier(0.33, 1, 0.68, 1) both;
  cursor: pointer;
  pointer-events: auto;
`

const SpotlightSnapshot = styled.img`
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
  object-fit: fill;
  pointer-events: none;
`

const PreparingLabel = styled.div`
  position: absolute;
  left: 50%;
  bottom: 8px;
  transform: translateX(-50%);
  padding: 3px 7px;
  border-radius: 3px;
  color: white;
  background: rgba(0, 0, 0, 0.7);
  font-size: 12px;
  white-space: nowrap;
  pointer-events: none;
`

const StreamTitle = styled.div`
  position: absolute;
  z-index: 100;
  pointer-events: auto;
  font-size: ${({ fontSize = 16 }) => `${fontSize}px`};
  ${({ position }) => {
    if (position === 'top-left') {
      return `top: 10px; left: 10px;`
    } else if (position === 'top-right') {
      return `top: 10px; right: 10px;`
    } else if (position === 'bottom-right') {
      return `bottom: 10px; right: 10px;`
    } else if (position === 'bottom-left') {
      return `bottom: 10px; left: 10px;`
    }
  }}
  max-width: calc(100% - 30px);
  box-sizing: border-box;

  display: inline-flex;
  align-items: center;
  justify-content: center;
  padding: 4px 10px;
  font-weight: 600;
  color: white;
  text-shadow: 0 0 4px black;
  letter-spacing: -0.025em;
  background: ${({
    isListening,
    activeColor,
  }: {
    isListening: boolean
    activeColor: string
  }) =>
    Color(isListening ? activeColor : 'black')
      .alpha(0.5)
      .toString()};
  border-radius: 4px;
  backdrop-filter: blur(10px);
  overflow: hidden;

  span {
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
  }

  svg {
    width: 1.25em;
    height: 1.25em;
    overflow: visible;
    filter: drop-shadow(0 0 4px black);

    &:first-child {
      margin-right: 0.35em;
    }

    &:last-child {
      margin-left: 0.5em;
    }

    path {
      fill: white;
    }
  }
`

const LoadingSpinner = styled(TailSpin)`
  position: absolute;
  left: 50%;
  top: 50%;
  transform: translate(-50%, -50%);
  width: 100px;
  height: 100px;
  opacity: 0.5;
`

const BlurCover = styled.div`
  position: absolute;
  left: -2px;
  right: -2px;
  top: -2px;
  bottom: -2px;
  pointer-events: none;
  backdrop-filter: blur(8px);
`

const VersionText = styled.div`
  position: fixed;
  bottom: 4px;
  right: 4px;
  color: white;
  font-size: 12px;
  text-shadow:
    0 0 1px rgba(0, 0, 0, 0.5),
    1px 0 1px rgba(0, 0, 0, 0.5),
    0 1px 1px rgba(0, 0, 0, 0.5),
    1px 1px 1px rgba(0, 0, 0, 0.5);
  backdrop-filter: blur(30px);
  padding: 1px 4px;
  border-bottom-left-radius: 4px;
  opacity: ${({ isShowing }) => (isShowing ? '.65' : '.35')};
  transition: ease-out 500ms all;
`

const OverlayIFrame = styled.iframe`
  position: fixed;
  left: 0;
  top: 0;
  width: 100vw;
  height: 100vh;
  border: none;
  pointer-events: none;
`

const TileOverlayClip = styled.div`
  position: absolute;
  inset: 0;
  overflow: hidden;
  pointer-events: none;
  z-index: -1;
`

const TileOverlay = styled.iframe`
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
  border: none;
  background: transparent;
  pointer-events: none;
  transform-origin: center center;
`

render(<App />, document.body)
