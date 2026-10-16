# Streamwall

:construction: Streamwall v2.0 is a work-in-progress :construction:

Goals for the v2 branch:

- TypeScript
- Use Electron Forge to distribute packaged releases
- Split out control server; refactor for local-only use without a webserver

---

Streamwall makes it easy to compose multiple livestreams into a mosaic, with source attributions and audio control.


## How it works

Under the hood, think of Streamwall as a specialized web browser for mosaicing video streams. It uses [Electron](https://www.electronjs.org) to create a grid of web browser views, loading the specified webpages into them. Once the page loads, Streamwall finds the `<video>` tag and reformats the page so that the video fills the space. This works for a wide variety of web pages without specialized scrapers.


## Configuration

Streamwall has a growing number of configuration options. To get a summary run:

```
npm start -- --help
```

For long-term installations, it's recommended to put your options into a configuration file. To use a config file, run:

```
npm start -- --config="../streamwall.toml"
```

See `example.config.toml` for an example.

### Video decoding on large grids

If many videos have buffered data but display few frames, try software video
decoding. Add this to Streamwall's `config.toml`:

```toml
[performance]
software-video-decode = true
```

Or add `--performance.software-video-decode=true` to the Streamwall shortcut.
This setting takes effect at launch and uses more CPU. Set it to `false` in
`config.toml` or pass `--performance.software-video-decode=false` to return to
the default hardware decode path. For a frame-rate
comparison, add `--media-health-log` to write rolling samples to
`%APPDATA%\Streamwall\media-health.jsonl`.

### Nearby camera recovery

The setup window can enable **Use the nearest available camera after three
failed starts**. The equivalent `config.toml` setting is:

```toml
[recovery]
nearby-fallback = true
```

This applies to geolocated video and HDHomeRun sources. After three failed
starts of the assigned stream, Streamwall tries the nearest camera that is not
already assigned on that grid. Failed alternatives are skipped. The saved
layout retains the original stream. While a replacement plays, Streamwall
checks the original in a muted background player (at most two checks across
all grids) and restores it after playback starts. The feature is off by
default; set `nearby-fallback = false` to disable it.

## Data sources

Streamwall can load stream data from both JSON APIs and TOML files. Data sources can be specified in a config file (see `example.config.toml` for an example) or the command line:

```
npm start -- --data.json-url="https://your-site/api/streams.json" --data.toml-file="./streams.toml"
```

### HDHomeRun channels

Add a channel from the control window's **Custom Streams** section by choosing
**HDHomeRun**, or add it to `streams.toml` in the app's user data folder:

```toml
[[streams]]
kind = "hdhomerun"
label = "Channel 2.1"
link = "http://192.0.2.10:5004/auto/v2.1"
source = "HDHomeRun"
```

Standard HDHomeRun URLs on port 5004 with `/auto/v...` or `/auto/ch...` are also
recognized when their kind is `video`. Streamwall starts one local FFmpeg
conversion per distinct channel used on the grid and stops it when the last
view closes. H.264 video is copied without re-encoding; other video codecs are
converted to H.264 for the built-in HLS player. The Windows installer includes
FFmpeg, so no separate command or service is needed.

### Transparent tile HTML overlays

Use **Tile HTML overlay** in the control window or web interface. Choose a stream,
paste the graphics page's HTTP(S) URL, and select **Save overlay**.
Search the picker by ID, label, source, location, or URL. Use **−5%** /
**+5%**, enter a percentage (10–300%), or **Reset 100%**, then **Save overlay** to
apply it. Scaling is centered and clipped to the tile without reloading the page.
**Remove overlay**
disables it. The setting is saved on this installation and follows the stream
across grid positions and enlarged views. Streams with the same URL and crop share
the setting. Only displayed streams load graphics; ordinary state updates do not
reload them. Existing full-window `kind = "overlay"` sources remain supported.

Cells with a configured overlay show a **layers** button in the control grid.
Click it to show or hide that cell's graphics, including in its enlarged view.
The button is highlighted while graphics are visible and is absent on cells
without an overlay. Visibility is saved per grid cell on this installation;
merged cells toggle together. Hidden graphics stay loaded for quick toggling,
while the underlying video keeps playing.

To define an overlay alongside a source in `streams.toml`:

```toml
[[streams]]
kind = "video"
label = "Camera with graphics"
link = "https://example.com/live.m3u8"
overlayUrl = "https://example.com/graphics/live"
overlayScale = 100 # percent; optional
```

Control interface settings override the TOML value for that URL and crop, including
an explicit removal. These settings live in the application's saved data; exporting
`streams.toml` exports the source file, so copy `overlayUrl` and `overlayScale` there when transferring
the configuration to another installation.

The page fills the tile and receives its current viewport size. Use responsive
graphics with a transparent HTML/canvas background. Overlays are visual only:
clicks continue to operate the tile and enlarged view. Embedding restrictions set
by the graphics provider (or by a third-party web stream page) still apply.

## Hotkeys

The following hotkeys are available with the "control" webpage focused:

- **alt+[1...9]**: Listen to the numbered stream
- **alt+shift+[1...9]**: Toggle blur on the numbered stream
- **alt+s**: Select the currently focused stream box to be swapped
- **alt+c**: Activate [Streamdelay](https://github.com/chromakode/streamdelay) censor mode
- **alt+shift+c**: Deactivate [Streamdelay](https://github.com/chromakode/streamdelay) censor mode
