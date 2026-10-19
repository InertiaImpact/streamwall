import { MakerDeb } from '@electron-forge/maker-deb'
import { MakerRpm } from '@electron-forge/maker-rpm'
import { MakerSquirrel } from '@electron-forge/maker-squirrel'
import { MakerZIP } from '@electron-forge/maker-zip'
import { FusesPlugin } from '@electron-forge/plugin-fuses'
import { VitePlugin } from '@electron-forge/plugin-vite'
import type { ForgeConfig } from '@electron-forge/shared-types'
import { FuseV1Options, FuseVersion } from '@electron/fuses'
import { execFileSync } from 'child_process'
import { existsSync, realpathSync } from 'fs'
import path from 'path'

function windowsFfmpegResources() {
  if (process.platform !== 'win32') return []
  let commandPath = process.env.STREAMWALL_FFMPEG_PATH
  if (!commandPath) {
    try {
      commandPath = execFileSync('where.exe', ['ffmpeg'], {
        encoding: 'utf8',
      })
        .trim()
        .split(/\r?\n/)[0]
    } catch {
      // Developer startup can run without HDHomeRun support until FFmpeg is installed.
      if (process.argv.includes('start')) return []
      throw new Error('FFmpeg is required for the Windows installer')
    }
  }
  const ffmpegPath = realpathSync(commandPath)
  if (!existsSync(ffmpegPath))
    throw new Error('FFmpeg is required for the Windows installer')
  return [
    ffmpegPath,
    path.join(__dirname, 'third_party', 'FFmpeg-GPL-3.0.txt'),
    path.join(__dirname, 'third_party', 'FFmpeg-NOTICE.txt'),
  ]
}

const config: ForgeConfig = {
  packagerConfig: {
    executableName: 'streamwall',
    icon: process.platform === 'win32' ? path.join(__dirname, 'assets', 'streamwall.ico') : undefined,
    asar: true,
    extraResource: [
      // Bundle the control client static files so the built app can serve them
      '../streamwall-control-client/dist',
      path.join(__dirname, 'assets', 'streamwall.ico'),
      ...windowsFfmpegResources(),
    ],
  },
  rebuildConfig: {},
  makers: [
    new MakerSquirrel({
      name: 'streamwall',
      setupIcon: path.join(__dirname, 'assets', 'streamwall.ico'),
      iconUrl: 'https://raw.githubusercontent.com/InertiaImpact/streamwall/performance-updates/packages/streamwall/assets/streamwall.ico',
    }),
    new MakerZIP({}, ['darwin']),
    new MakerRpm({}),
    new MakerDeb({}),
  ],
  publishers: [
    {
      name: '@electron-forge/publisher-github',
      config: {
        repository: {
          owner: 'streamwall',
          name: 'streamwall',
        },
        prerelease: true,
      },
    },
  ],
  plugins: [
    new VitePlugin({
      build: [
        {
          entry: 'src/main/index.ts',
          config: 'vite.main.config.ts',
          target: 'main',
        },
        {
          entry: 'src/preload/layerPreload.ts',
          config: 'vite.preload.config.ts',
          target: 'preload',
        },
        {
          entry: 'src/preload/mediaPreload.ts',
          config: 'vite.preload.config.ts',
          target: 'preload',
        },
        {
          entry: 'src/preload/spotlightOverlayPreload.ts',
          config: 'vite.preload.config.ts',
          target: 'preload',
        },
        {
          entry: 'src/preload/controlPreload.ts',
          config: 'vite.preload.config.ts',
          target: 'preload',
        },
      ],
      renderer: [
        {
          name: 'main_window',
          config: 'vite.renderer.config.ts',
        },
      ],
    }),
    new FusesPlugin({
      version: FuseVersion.V1,
      [FuseV1Options.RunAsNode]: false,
      [FuseV1Options.EnableCookieEncryption]: true,
      [FuseV1Options.EnableNodeOptionsEnvironmentVariable]: false,
      [FuseV1Options.EnableNodeCliInspectArguments]: false,
      [FuseV1Options.EnableEmbeddedAsarIntegrityValidation]: false,
      [FuseV1Options.OnlyLoadAppFromAsar]: false,
    }),
  ],
}

export default config
