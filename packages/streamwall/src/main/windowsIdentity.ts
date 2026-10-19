import { app } from 'electron'
import { execFile } from 'node:child_process'
import { existsSync } from 'node:fs'
import path from 'node:path'

// Matches Squirrel's package ID and executable name; keep stable across updates.
const APP_ID = 'com.squirrel.streamwall.streamwall'
const UNINSTALL_KEY = 'HKCU\\Software\\Microsoft\\Windows\\CurrentVersion\\Uninstall\\streamwall'

export function configureWindowsIdentity() {
  if (process.platform !== 'win32') return
  if (process.argv.some((arg) => arg === '--squirrel-uninstall' || arg === '--squirrel-obsolete')) return
  app.setAppUserModelId(APP_ID)
  const icon = app.isPackaged
    ? path.join(process.resourcesPath, 'streamwall.ico')
    : path.join(__dirname, '../../assets/streamwall.ico')
  if (!existsSync(icon)) return
  app.on('browser-window-created', (_event, window) => {
    window.setIcon(icon)
    window.setAppDetails({ appId: APP_ID })
  })

  // Squirrel retains an existing downloaded app.ico, including an old Electron
  // icon. Point our own installed-program entry at the bundled icon instead.
  // This also works offline and leaves portable/development builds untouched.
  if (!app.isPackaged ||
    !existsSync(path.join(path.dirname(process.execPath), '..', 'Update.exe'))) return
  const displayIcon = `"${icon}",0`
  execFile('reg.exe', ['query', UNINSTALL_KEY], { windowsHide: true }, (error, output) => {
    if (error || output.toLowerCase().includes(displayIcon.toLowerCase())) return
    execFile('reg.exe', ['add', UNINSTALL_KEY, '/v', 'DisplayIcon', '/t', 'REG_SZ',
      '/d', displayIcon, '/f'], { windowsHide: true }, (writeError) => {
      if (writeError) console.warn('[windows] Could not update installed-program icon', writeError.message)
    })
  })
}
