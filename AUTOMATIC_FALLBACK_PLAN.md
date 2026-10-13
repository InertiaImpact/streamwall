# Location-based stream fallback

Status: implemented on `performance-updates` behind the opt-in
`[recovery] nearby-fallback = true` setting. The remote machine has not been
updated with this version.

## Why this needs a separate change

The source list already includes latitude and longitude, and each tile reports
loading, running, or error. The HLS renderer also reports startup failures and
terminal stalls. The current layout stores the chosen stream ID in the shared
state document. Replacing that ID would overwrite the operator's layout, so
fallback must be a temporary runtime assignment with the original ID retained.

On the observed remote instance, most cameras have unknown/offline list status.
List status alone cannot decide whether a nearby camera will play.

## Approaches

| Approach | Benefit | Cost / limitation |
| --- | --- | --- |
| Pick the nearest source marked Live | Little extra work | Excludes many playable cameras whose list status is unknown; does not prove playback |
| Try nearby cameras in distance order | Uses actual playback result; no idle decoders | Replacement may take several startup attempts |
| Keep nearby cameras and the original in hidden players | Fastest switch and return | Adds renderers, decoding, bandwidth, and possibly HDHomeRun tuner use to an already large grid |

Use serial candidate startup and a bounded recovery probe for the original.
The substitute remains visible while recovery is checked. Do not keep a pool
of hidden video decoders running.

## Implemented behavior

1. Make the feature opt-in for the app, default off. Keep the stored layout's
   stream ID as the **desired** stream; track an **effective** stream separately
   for playback. Manual assignment or layout load cancels pending fallback.
2. Count an app-level failed start when a view reaches `error` or fails to
   reach `running` after a bounded startup deadline. A startup retry is a new
   view load; hls.js's internal fragment retries are not counted separately.
   After three app-level failed starts of the desired source, find a backup.
   A stream that had been running and later reaches a terminal playback error
   enters the same three-attempt recovery path. Ordinary brief stalls and low
   frame rate do not trigger replacement.
3. Rank eligible, geolocated video sources by Haversine distance from the
   desired source. Skip the same stream, duplicate coordinates at the same
   location, every stream already assigned or reserved on that grid, and
   candidates in a recent failure cooldown. Include sources with unknown list
   status because playback is the authoritative check. Streams without valid
   coordinates keep the current error/retry behavior.
4. Start one candidate at a time per failed view through the existing startup scheduler. If
   it fails, apply a cooldown and try the next closest. Reserve a candidate
   while it is loading so two failed tiles cannot claim it. Release the
   reservation when it fails or the tile's desired assignment changes. If the
   list is exhausted, retry the desired source on a bounded schedule without
   a tight loop.
5. While a backup plays, try the desired source in a muted, offscreen player.
   At most two such probes run across all grids. A probe must reach `running`
   and advance video time before it can replace the backup. Failed probes use
   exponential backoff. The backup remains visible until the verified view is
   promoted into its position.
6. Mark the replacement in the grid overlay and control UI; log the original,
   replacement, and distance. Manual source selection always wins. Save/export
   still records only the desired stream.

## Boundaries and acceptance checks

- No automatic replacement for non-geolocated streams or non-video content.
- Limit simultaneous recovery loads and probes globally. Never launch all
  candidate players at once.
- Verify three failed desired starts, nearest occupied candidate, failed
  candidate, simultaneous failed tiles, source-list edits, manual changes,
  full-grid layouts, and successful restoration.
- Verify a low-frame-rate but advancing video does not trigger fallback.
- Compare process count, memory, bandwidth, and tuner occupancy against the
  same grid with fallback disabled before enabling it on the remote machine.
