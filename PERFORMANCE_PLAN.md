# Large-grid playback performance plan

Status: planned. The remote machine has not been changed by this plan.

## Observed baseline (2026-10-13)

The remote Streamwall sampled for this plan runs version `2.4.0-bs` with
three grids containing 49, 19, and 25 assigned views. At the time sampled,
84 were running, three were loading, and six were in error. Of 93 assigned
views, 92 were Iowa DOT HLS cameras and one was an HDHomeRun URL.

Electron reported 85 app processes, including 82 renderers, about 11.2 GB
total working set, and 1.43 GB in the GPU process. System memory had about
16.6 GB free. A sample Iowa DOT master playlist advertised one 480x270,
15 fps variant. Grid 1's recent 250 diagnostic entries included many
`bufferStalledError` events, while sampled HLS request scheduler entries
showed zero rejections and queue waits near zero. These observations do not
yet distinguish decode, compositor, renderer main-thread, timestamp, or
source cadence problems. Overall CPU/GPU percentages cannot resolve them.

The app already uses separate Electron renderers, six persistent HLS media
sessions, bounded HLS request scheduling, and startup admission. HLS.js
transmux workers are currently disabled because each tile already owns a
renderer. The configured Chromium renderer process limit is 16, but the
remote app reports 82 renderer processes; verify its actual effect before
changing that switch.

## Success criteria and comparison method

Use the same 93-tile layout and a fixed 30-minute camera set for each
comparison. Record source-advertised fps and delivered fps for each tile.
Track the percentage of tiles below 90% of source fps, dropped-frame rate,
stall seconds per tile-hour, failed starts, renderer crashes, startup time,
and p95 control responsiveness. Also record app/renderer/GPU working sets,
per-process CPU, and GPU video-decode activity. A change ships only if it
improves delivered frames or stall time without worsening failed starts or
control responsiveness. Keep each change behind a rollback flag until the
remote comparison is complete.

## Phase 1: find the limiting stage

1. Add a lightweight, opt-in media health sample every 10 seconds, aggregated
   by grid. For each video, report `getVideoPlaybackQuality()` frame deltas,
   `requestVideoFrameCallback()` presentation cadence when available,
   dimensions, ready state, buffered seconds, playback rate, and HLS level.
   Do not send frame-by-frame IPC or retain unlimited per-tile history.
2. Extend the existing runtime diagnostics with per-process CPU and memory,
   renderer PID to view mapping, main-process event-loop delay, and
   `app.getGPUFeatureStatus().video_decode`. Capture GPU video-decode engine
   activity from Windows during the remote trial.
3. Separate source stalls from local pipeline stalls: if segments arrive on
   time and buffers stay ahead while presentation frames stop, investigate
   decode/composition/timestamp handling; if buffers empty or request waits
   rise, investigate origin/network admission instead. Compare the measured
   cadence against the source's 15 fps before calling it a frame drop.

## Phase 2: low-risk renderer and overlay reductions

1. Stop creating a full-tile `backdrop-filter: blur(0)` layer for every
   unblurred stream. Profile removal of decorative label blur on a 7x7 grid.
2. Send the overlay only the grid/view and label data it needs. Avoid
   repeatedly serializing all ~694 stream definitions to every grid after
   individual view state changes. Coalesce state updates to a bounded rate
   while delivering control changes promptly.
3. Measure the main-process cost of per-fragment HLS scheduler IPC and debug
   logging. Keep the current fairness guarantees; optimize only if profiling
   shows measurable event-loop delay.

## Phase 3: bounded HLS renderer consolidation (prototype)

Prototype media hosts containing roughly 6-10 HLS video elements each,
with several hosts per grid rather than one renderer per camera. Keep
arbitrary web pages in isolated WebContentsViews. The media host owns tile
positioning, crop, mute, errors, and per-video health; spotlight repositions
the existing video inside its host without a second stream. Preserve the
existing control and stream TOML model. Start with one grid behind an
opt-in flag and compare host sizes of 4, 8, and 12 videos. Do not force all
93 players onto one renderer main thread.

Only after measuring the consolidated host should we test HLS.js transmux
workers. A worker per camera may trade renderer contention for dozens of
extra threads; compare worker enabled/disabled under identical layouts.
Retain per-tile renderer mode as the fallback for websites and problematic
feeds. Stop the prototype if frame delivery, stability, or spotlight behavior
regresses despite a lower process count.

## Phase 4: adaptive quality where sources permit it

Keep `capLevelToPlayerSize` for multi-rendition HLS. For sources with multiple
variants, prefer a tile-sized rendition and allow spotlight to request a
higher one. The sampled Iowa DOT feed has only one 480x270 variant, so this
cannot reduce its decode load. Avoid scaling a high-resolution decode in CSS
as a substitute for selecting a lower encoded rendition.

## Rollout order

Ship the label fix separately. Deploy Phase 1 instrumentation to the remote
machine, establish the 30-minute baseline, then trial Phase 2 changes one at
a time. Attempt Phase 3 only if per-tile renderer or composition cost remains
material. Each trial records its version, flags, layout, and result before
promoting or reverting it.
