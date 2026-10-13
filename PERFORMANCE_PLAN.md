# Large-grid playback performance plan

Status: local baseline captured; the remote machine has not been changed by
this plan.

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

### Local 12×12 stress window (2026-10-13)

The local `2.4.4-bs` app had 61 assigned views in the 12×12 grid and seven in
another grid when sampled. A 12×12 layout defines capacity; this run did not
yet exercise 144 playing streams. Windows reported 75 renderer processes and
about 9.8 GB total Streamwall working set, including about 1.0 GB in the GPU
process. The machine had 24.6 GB free of 63.7 GB physical memory. A corrected
10-second CPU sample showed about 3.1 CPU cores used by Streamwall, 1.8 of
them in the GPU process. One GPU engine sample showed about 43% video decode
and 11% 3D utilization. These snapshots do not establish frame delivery or
exclude short bursts of saturation; collect per-video frame and buffer data
before attributing the reported stutter to a specific stage.

After restarting the local app into the opt-in diagnostic package, the 12×12
grid had 90 active views; 85 video views supplied frame samples. The other
4×5 grid had seven sampled video views. At 17:20:28 UTC, the first 28 sampled
videos in the large grid averaged 15.6 fps. From 17:20:58 through 17:25:28
UTC, 30-second snapshots repeatedly showed large-grid means around 2–4 fps,
with one temporary rise. During the low-fps periods, sampled videos averaged
roughly 8–10 seconds buffered and their media clocks advanced roughly 9–10
seconds per 10-second sample. The small grid slowed at the same time. This
favors a shared local decode/presentation problem over an origin or request
admission bottleneck, but does not yet isolate a GPU engine, thread, driver,
decoder-session limit, or Chromium compositor cause. With 90 active views,
Streamwall used about 10.5 GB working set and 4.95 CPU cores in one sample;
the GPU process used 2.4 of those cores. A GPU engine sample showed 53.5%
video decode and 18.3% 3D utilization.

### Local decode A/B (same 90-view layout)

Ten consecutive 30-second snapshots with hardware decode averaged roughly
2.7–3.7 fps across 85 sampled videos in the 12×12 grid. After a graceful
restart with Chromium's `--disable-accelerated-video-decode`, five consecutive
snapshots held at 16.5–16.8 fps with no more than one video below 5 fps. A GPU
engine sample showed no video-decode activity in the software run. One process
sample showed about 11.0 CPU cores and 11.7 GB working set, compared with
about 5.0 cores and 10.5 GB in the hardware run. This is strong evidence that
the local hardware-decode path is the limiting stage; it does not establish
that other machines or codecs will benefit. Keep software decode opt-in in
`config.toml` or with `--performance.software-video-decode=true` and compare it
on the remote machine before changing any default.

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
   by grid. The first implementation uses `getVideoPlaybackQuality()` frame
   deltas, dimensions, ready state, buffered seconds, playback rate, and media
   time advancement for video, HLS, and HDHomeRun views. Launch with
   `--skip-setup=true --media-health-log` to write a rolling, 8 MB
   `media-health.jsonl` in the Streamwall user data directory. It emits one
   snapshot every 30 seconds, with no frame-by-frame IPC. Compare a fixed
   layout against the uninstrumented baseline before enabling callback-based
   presentation cadence or adding HLS level telemetry.
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
   unblurred stream. This code change is prepared on `performance-updates` for
   comparison against the running `2.4.4-bs` baseline. Profile removal of
   decorative label blur separately on the same grid.
2. Send the overlay only the grid/view and label data it needs. Avoid
   repeatedly serializing all ~694 stream definitions to every grid after
   individual view state changes. Coalesce state updates to a bounded rate
   while delivering control changes promptly.
3. Measure the main-process cost of per-fragment HLS scheduler IPC and debug
   logging. Keep the current fairness guarantees; optimize only if profiling
   shows measurable event-loop delay.

## Phase 3: bounded HLS renderer consolidation (prototype)

If the remote software-decode trial still has frame delivery or process-memory
problems, prototype media hosts containing roughly 6-10 HLS video elements each,
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

Version `2.5.0-bs` contains the opt-in software-decode setting and frame
diagnostics. The local 90-view comparison passed, and the installed build
retained the frame-rate improvement. On the remote machine, compare the same
fixed camera set for 30 minutes with the setting off and on. Record frame
delivery, stalls, CPU, memory, and startup failures. Keep the setting opt-in
until that comparison is complete. Attempt Phase 3 only if frame delivery or
process-memory cost remains material after the remote decode trial. Each trial
records its version, flags, layout, and result before promoting or reverting
it.
